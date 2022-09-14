import datetime
import logging
import os
import time
from typing import List

from galileocontext.constants import zone_label, api_gateway_type_label, pod_type_label, function_label
from galileocontext.context.cluster import Context
from galileocontext.deployments.api import Deployment
from lbopt.latency import LogisticFunctionParameters, logistic_curve
from lbopt.opt.api import WeightOptimizer
from lbopt.opt.threshold import ThresholdBasedWeightOptimizerParameters, calculate_diff
from lbopt.weights.api import WeightUpdate, Weights

logger = logging.getLogger(__name__)


class ClusteredThresholdBasedWeightOptimizer(WeightOptimizer):
    """
    Currently only cpu threshold considered
    """

    def __init__(self, parameters: ThresholdBasedWeightOptimizerParameters):
        self.parameters = parameters

    def run(self, ctx: Context) -> List[List[WeightUpdate]]:
        try:
            deployments = ctx.deployment_service.get_deployments()
            logger.debug(f"run threshold based weight optimizer, for deployments: {deployments}")
            weights = []
            for zone in ctx.zone_service.get_zones():
                for deployment in deployments:
                    if deployment.labels.get(function_label, None) is None:
                        continue
                    logger.info(f"execute optimization for deployment {deployment.fn_name}")
                    deployment_weights = self._run_deployment(deployment, zone, ctx)
                    logger.info(f"finished optimization for deployment {deployment.fn_name}")
                    logger.debug(f"resulting weights: {deployment_weights}")
                    weights.append(deployment_weights)
            return weights
        except Exception as e:
            logger.error(e)
            return []

    def _run_deployment(self, deployment: Deployment, zone: str, ctx: Context) -> List[WeightUpdate]:
        """
        probability: thr_f - util(f_i) / (sum_j(thr_f - util(f_j))
        """
        fn = deployment.fn_name

        pod_containers = ctx.pod_service.find_pod_containers_with_labels(
            labels={
                function_label: fn
            },
            node_labels={
                zone_label: zone
            }
        )

        ips = []
        weights = []
        weight_updates = []
        now = datetime.datetime.utcnow()
        lookback_seconds_ago = now - datetime.timedelta(seconds=self.parameters.lookback)

        try:
            api_gateway = ctx.pod_service.find_pod_containers_with_labels(
                labels={pod_type_label: api_gateway_type_label}, node_labels={zone_label: zone})[0]
        except IndexError:
            raise ValueError(f'Could not find api gateway for zone {zone}')
        if api_gateway is None == 0:
            return []

        a = self.parameters.log_parameters.a
        b = self.parameters.log_parameters.b
        c = self.parameters.log_parameters.c
        offset = self.parameters.log_parameters.offset
        midpoint = self.parameters.log_parameters.midpoint
        d = midpoint - (midpoint * offset)

        for pod_container in pod_containers:
            diff = calculate_diff(ctx, pod_container.container_id, lookback_seconds_ago, now)
            if diff == 0:
                diff = 1
            latency = ctx.network_service.get_latency(pod_container.nodeName, api_gateway.nodeName)
            if latency == 0:
                latency = 0.01
            p_lat_a_x_f = max(0.01, 1 - logistic_curve(latency, a, b, c, d) / a)
            logger.info(f'Pod: {pod_container.name}, lat:{latency}, log: {p_lat_a_x_f}, cpu: {diff}')
            p = diff * p_lat_a_x_f
            ips.append(pod_container.url)
            # wrr works with integer
            weights.append(int(p * 100))

        # using the same timestamp will result in InfluxDB overwriting the previous entry
        ts = time.time()
        weights = Weights(ips, weights)

        weight_update = WeightUpdate(
            ts=ts,
            fn=fn,
            zone=zone,
            weights=weights
        )
        weight_updates.append(weight_update)

        return weight_updates

    @classmethod
    def read_from_env(cls) -> 'ClusteredThresholdBasedWeightOptimizer':
        log_parameters = LogisticFunctionParameters.read_from_env()
        lookback = int(os.environ['osmotic_lb_opt_lookback'])
        parameters = ThresholdBasedWeightOptimizerParameters(log_parameters, lookback)
        return ClusteredThresholdBasedWeightOptimizer(parameters)


if __name__ == '__main__':
    opt = ClusteredThresholdBasedWeightOptimizer.read_from_env()
    print(opt)
