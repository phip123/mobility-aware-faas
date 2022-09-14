import datetime
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np

from galileocontext.constants import zone_label, api_gateway_type_label, pod_type_label, function_label
from galileocontext.context.cluster import Context
from galileocontext.deployments.api import Deployment
from galileocontext.pod.api import PodContainer
from lbopt.latency import logistic_curve, LogisticFunctionParameters
from lbopt.opt.api import WeightOptimizer
from lbopt.weights.api import WeightUpdate, Weights

logger = logging.getLogger(__name__)


@dataclass
class ThresholdBasedWeightOptimizerParameters:
    log_parameters: LogisticFunctionParameters
    lookback: int


class ThresholdBasedWeightOptimizer(WeightOptimizer):
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
            for deployment in deployments:
                if deployment.labels.get(function_label, None) is None:
                    continue
                logger.info(f"execute optimization for deployment {deployment.fn_name}")
                deployment_weights = self._run_deployment(deployment, ctx)
                logger.info(f"finished optimization for deployment {deployment.fn_name}")
                logger.debug(f"resulting weights: {deployment_weights}")
                weights.append(deployment_weights)
            return weights
        except Exception as e:
            logger.error(e)
            return []

    def _run_deployment(self, deployment: Deployment, ctx: Context) -> List[WeightUpdate]:
        """
        probability: thr_f - util(f_i) / (sum_j(thr_f - util(f_j))
        external api gateway probability: calculate probability for all functions and each api gateway gets
        the average over all internal ones favors forward to apigateways that contain mainly unused
        zones, but will increase pressure therefore may spawn enw instance in own region
         """
        pod_containers = ctx.pod_service.get_pod_containers_of_deployment(deployment.original_name)
        fn = deployment.fn_name
        ips_per_zone = defaultdict(list)
        weights_per_zone = defaultdict(list)
        all_ws = []
        zones = ctx.zone_service.get_zones()

        # pods_per_zone = get_pods_per_zone(pod_containers, zones)
        now = datetime.datetime.utcnow()
        lookback_seconds_ago = now - datetime.timedelta(seconds=self.parameters.lookback)

        api_gateways_per_zone: Dict[str, PodContainer] = {}
        for zone in zones:
            try:
                api_gateways_per_zone[zone] = ctx.pod_service.find_pod_containers_with_labels(
                    labels={pod_type_label: api_gateway_type_label}, node_labels={zone_label: zone})[0]
            except IndexError:
                raise ValueError(f'Could not find api gateway for zone {zone}')
        if len(api_gateways_per_zone) == 0:
            return []

        a = self.parameters.log_parameters.a
        b = self.parameters.log_parameters.b
        c = self.parameters.log_parameters.c
        offset = self.parameters.log_parameters.offset
        midpoint = self.parameters.log_parameters.midpoint
        d = midpoint - (midpoint * offset)

        # this can be done on each load balancer/gateway
        for pod_container in pod_containers:
            diff = calculate_diff(ctx, pod_container.container_id, lookback_seconds_ago, now)
            if diff == 0:
                diff = 1
            node = ctx.node_service.find(pod_container.nodeName)
            zone = node.labels[zone_label]
            api_gateway = api_gateways_per_zone.get(zone, None)
            if api_gateway is None:
                continue
            latency = ctx.network_service.get_latency(pod_container.nodeName, api_gateway.nodeName)
            if latency == 0:
                latency = 0.01

            p_lat_a_x_f = max(0.01, 1 - logistic_curve(latency, a, b, c, d) / a)
            logger.info(f'Pod: {pod_container.name}, lat:{latency}, log: {p_lat_a_x_f}, cpu: {diff}')

            p = diff * p_lat_a_x_f
            all_ws.append((pod_container, p))
            ips_per_zone[zone].append(pod_container.url)
            # wrr works with integer
            weights_per_zone[zone].append(int(p * 100))

        # this requires communication between them
        p_zone = dict()
        for zone in zones:
            p_zone[zone] = calculate_p_for_zone(zone, all_ws, ctx)

        weight_updates = []
        for zone in zones:
            for other_zone in zones:
                if zone == other_zone:
                    continue
                other_api_gateway = api_gateways_per_zone[other_zone]
                zone_api_gateway = api_gateways_per_zone[zone]
                zone_node_name = zone_api_gateway.nodeName
                other_node_name = other_api_gateway.nodeName
                latency = ctx.network_service.get_latency(zone_node_name, other_node_name)
                logger.info(f'From: {zone} to: {other_zone}, lat:{latency}')
                if latency == 0:
                    p_lat_a_x_f = 0.01
                else:
                    p_lat_a_x_f = max(0.01, 1 - logistic_curve(latency, a, b, c, d) / a)

                p_e_a = p_zone[other_zone] * p_lat_a_x_f
                # p_zone[other_zone] can be 0 when no pod is hosted in that zone
                if p_zone[other_zone] == 0:
                    # ignore zones where no pod is hosted
                    continue
                else:
                    ips_per_zone[zone].append(other_api_gateway.url)
                    p = int(p_e_a)
                    if p == 0:
                        p = 1
                    weights_per_zone[zone].append(p)

        for zone in zones:
            # using the same timestamp will result in InfluxDB overwriting the previous entry
            ts = time.time()
            weights = Weights(ips_per_zone[zone], weights_per_zone[zone])

            weight_update = WeightUpdate(
                ts=ts,
                fn=fn,
                zone=zone,
                weights=weights
            )
            weight_updates.append(weight_update)

        return weight_updates

    @classmethod
    def read_from_env(cls) -> 'ThresholdBasedWeightOptimizer':
        log_parameters = LogisticFunctionParameters.read_from_env()
        lookback = int(os.environ['osmotic_lb_opt_lookback'])
        parameters = ThresholdBasedWeightOptimizerParameters(log_parameters, lookback)
        return ThresholdBasedWeightOptimizer(parameters)


def calculate_p_for_zone(zone: str, weighted_pods: List[Tuple[PodContainer, float]], ctx: Context) -> float:
    nodes_of_pods = map(lambda w: (ctx.node_service.find(w[0].nodeName), w), weighted_pods)
    pods_in_zone = list(filter(lambda n: n[0].labels[zone_label] == zone, nodes_of_pods))
    n = len(pods_in_zone)
    if n == 0:
        return 0
    return sum((map(lambda p: p[1][1], pods_in_zone))) / n


def calculate_diff(ctx: Context, container_id: str, start: datetime.datetime, end: datetime.datetime) -> float:
    cpu = ctx.telemetry_service.get_container_cpu(container_id, start, end)
    # pod = ctx.pod_service.get_pod_container_with_id(container_id)
    if cpu is None:
        logger.error(f"Cpu for container {container_id} is None")
        return 1
    # kubernetes oriented usage vs. request utilization, cpu usage can go higher than 1
    # diff = 1 - (cpu['milli_cores'].mean() / pod.parsed_resource_requests.get('cpu'))

    # normalized to 1
    diff = 1 - cpu['percentage_relative'].mean()
    return diff


def get_pods_per_zone(pod_containers, zones: List[str], ctx: Context) -> Dict[str, List[PodContainer]]:
    pods_per_zone = defaultdict(list)
    for zone in zones:
        for pod_container in pod_containers:
            node = ctx.node_service.find(pod_container.nodeName)
            if node == zone:
                pods_per_zone[zone].append(pod_container)
    return pods_per_zone


if __name__ == '__main__':
    opt = ThresholdBasedWeightOptimizer.read_from_env()
    print(opt)
