import datetime
import logging
import math
import time
import uuid
from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np

from galileocontext.constants import pod_pending, function_label, zone_label
from galileocontext.context.cluster import Context
from galileocontext.deployments.api import Deployment
from schedulescaleopt.opt.api import ScalerSchedulerOptimizer, HlpaDecision, PodRequest, \
    ScaleScheduleEvent, HlpaScalerSchedulerResult


@dataclass
class HorizontalLatencyPodOptimizerSpec:
    target_deployment: str
    min_replicas: int
    max_replicas: Optional[int]
    # in seconds
    lookback: int
    # either latency (in ms) or rtt (in s)
    target_time_measure: str
    deployment_pattern: str = '-deployment'
    threshold_tolerance: float = 0.1
    # ms when latency, s when rtt
    target_duration: float = 100
    percentile_duration: float = 90


logger = logging.getLogger(__name__)


class HorizontalLatencyPodOptimizer(ScalerSchedulerOptimizer):
    """
    This Optimizer implementation is based on the official default Kubernetes HPA and uses CPU  usage to determine
    the number of Pods.

    Reference: https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/
    """

    def __init__(self, parameters: Dict[str, HorizontalLatencyPodOptimizerSpec]):
        """
        :param parameters: HPA Spec per deployment
        """
        self.parameters = parameters

    def run(self, ctx: Context) -> HlpaScalerSchedulerResult:
        """
        Implements a RTT-based scaling approach based on the HPA.

        In contrast to the official HPA, this implementation is not aggregating on a per-pod basis
        (i.e., average over all pods), but considers the available traces for each deployment over all traces.
        This means that we aggregate over all traces.


        The implementation considers all running Pods to be ready!

        In contrast to the official HPA, we cannot estimate the latency for Pods that have not yet received any requests.


        In the following the algorithm is described and parts of the documentation copied in case there are differences
        or notable changes to the official documentation.

        "When scaling on CPU, if any pod has yet to become ready (it's still initializing, or possibly is unhealthy)
        or the most recent metric point for the pod was before it became ready, that pod is set aside as well."
        - the implementation always considers all available CPU metrics

        "If there were any missing metrics, the control plane recomputes the average more conservatively, assuming
        those pods were consuming 100% of the desired value in case of a scale down, and 0% in case of a scale up.
        This dampens the magnitude of any potential scale."
        - the implementation considers this

        "Furthermore, if any not-yet-ready pods were present, and the workload would have scaled up without
        factoring in missing metrics or not-yet-ready pods, the controller conservatively assumes that the
        not-yet-ready pods are consuming 0% of the desired metric, further dampening the magnitude of a scale up."
        - the implementation considers this

        "After factoring in the not-yet-ready pods and missing metrics, the controller recalculates the usage ratio.
        If the new ratio reverses the scale direction, or is within the tolerance, the controller doesn't take any
        scaling action. In other cases, the new ratio is used to decide any change to the number of Pods."
        - the implementation considers this

        :param ctx:
        :return:
        """
        hlpa_decisions = []
        scale_schedule_events = []

        for deployment in ctx.deployment_service.get_deployments():
            logger.info(f'HLPA scaling for function {deployment.fn_name}')
            spec = self.parameters.get(deployment.fn_name, None)
            if spec is None:
                continue
            running_pods = ctx.pod_service.get_pod_containers_of_deployment(deployment.original_name)
            pending_pods = ctx.pod_service.get_pod_containers_of_deployment(deployment.original_name, running=False,
                                                                            status=pod_pending)
            no_of_running_pods = len(running_pods)
            no_of_pending_pods = len(pending_pods)
            no_of_pods = no_of_running_pods + no_of_pending_pods
            now = datetime.datetime.now()
            lookback_seconds_ago = now - datetime.timedelta(seconds=spec.lookback)
            logger.info(f"Fetch traces {spec.lookback} seconds ago")
            traces = ctx.trace_service.get_traces_for_deployment(deployment, lookback_seconds_ago, now)
            if len(traces) == 0:
                logger.info(f'No trace data for function: {deployment.fn_name}, skip iteration')
                hlpa_decision = HlpaDecision(
                    ts=time.time(),
                    fn=deployment.fn_name,
                    duration_agg=-1,
                    target_duration=spec.target_duration,
                    percentile=spec.percentile_duration,
                    base_scale_ratio=-1,
                    running_pods=no_of_running_pods,
                    pending_pods=no_of_pending_pods,
                    threshold_tolerance=spec.threshold_tolerance,
                    desired_replicas=no_of_pods,
                )
                hlpa_decisions.append(hlpa_decision)
                continue

            if spec.target_time_measure == 'rtt':
                traces[spec.target_time_measure] = traces[spec.target_time_measure] * 1000

            # percentile of rtt over all traces
            duration_agg = np.percentile(q=spec.percentile_duration, a=traces[spec.target_time_measure])
            mean = np.mean(a=traces[spec.target_time_measure])
            base_scale_ratio = duration_agg / spec.target_duration
            logger.info(
                f"Base scale ratio: {base_scale_ratio}."
                f" With a {spec.percentile_duration} percentile of {duration_agg} and a target of {spec.target_duration}, and a mean of {mean}")
            if 1 + spec.threshold_tolerance > base_scale_ratio > 1 - spec.threshold_tolerance:
                logger.info(
                    f'{spec.target_time_measure} percentile ({duration_agg}) was close enough to target ({spec.target_duration}) '
                    f'with a tolerance of {spec.threshold_tolerance}')
                hlpa_decision = HlpaDecision(
                    ts=time.time(),
                    fn=deployment.fn_name,
                    duration_agg=duration_agg,
                    target_duration=spec.target_duration,
                    base_scale_ratio=base_scale_ratio,
                    running_pods=no_of_running_pods,
                    pending_pods=no_of_pending_pods,
                    percentile=spec.percentile_duration,
                    threshold_tolerance=spec.threshold_tolerance,
                    desired_replicas=no_of_pods,
                )
                hlpa_decisions.append(hlpa_decision)
                continue

            desired_replicas = math.ceil(no_of_running_pods * (base_scale_ratio))
            if desired_replicas == no_of_running_pods:
                logger.info(f"No scaling actions necessary for {deployment.fn_name}")
                hlpa_decision = HlpaDecision(
                    ts=time.time(),
                    fn=deployment.fn_name,
                    duration_agg=duration_agg,
                    target_duration=spec.target_duration,
                    base_scale_ratio=base_scale_ratio,
                    running_pods=no_of_running_pods,
                    pending_pods=no_of_pending_pods,
                    percentile=spec.percentile_duration,
                    threshold_tolerance=spec.threshold_tolerance,
                    desired_replicas=desired_replicas,
                )
                hlpa_decisions.append(hlpa_decision)
                continue
            logger.info(f"Scale to {desired_replicas} from {no_of_running_pods} without "
                        f"factoring in not-yet-ready pods")
            if desired_replicas > no_of_running_pods:
                logger.info("Scale up")

                # check if new number of pods is over the maximum. if yes => set to minimum
                if spec.max_replicas is not None and desired_replicas > spec.max_replicas:
                    desired_replicas = spec.max_replicas

                scale_up_containers = desired_replicas - no_of_pods

                for i in range(0, scale_up_containers):
                    result = self.prepare_pod_request(deployment, spec.deployment_pattern)
                    scale_schedule_events.append(result)

            else:
                logger.info("Scale down")

                scale_down_containers = no_of_pods - desired_replicas

                # choose the last added containers
                # TODO: include pending pods, can lead to issues if too many pods
                #  are pending and not enoguh pods are running to remove
                to_remove = running_pods[no_of_running_pods - scale_down_containers:]
                for i in range(0, scale_down_containers):
                    remove = to_remove[i]
                    ts = time.time()
                    pod_request = PodRequest(
                        ts=ts,
                        pod_name=remove.name,
                        container_id=remove.container_id,
                        image=remove.image,
                        labels=remove.labels,
                        resource_requests=remove.resource_requests,
                        namespace=remove.namespace
                    )

                    event = ScaleScheduleEvent(
                        ts=ts,
                        fn=deployment.fn_name,
                        pod=pod_request,
                        origin_zone='',
                        dest_zone='',
                        delete=True
                    )

                    scale_schedule_events.append(event)

            hlpa_decision = HlpaDecision(
                ts=time.time(),
                fn=deployment.fn_name,
                duration_agg=duration_agg,
                target_duration=spec.target_duration,
                base_scale_ratio=base_scale_ratio,
                running_pods=no_of_running_pods,
                pending_pods=no_of_pending_pods,
                threshold_tolerance=spec.threshold_tolerance,
                desired_replicas=desired_replicas,
                percentile=spec.percentile_duration
            )

            hlpa_decisions.append(hlpa_decision)

        return HlpaScalerSchedulerResult(decisions=hlpa_decisions, scale_schedule_events=scale_schedule_events)

    def prepare_pod_request(self, fn_to_spawn: Deployment, deployment_pattern: str) -> ScaleScheduleEvent:
        pod_name_id = str(uuid.uuid4())[:5]
        fn = fn_to_spawn.labels[function_label]
        labels = fn_to_spawn.labels.copy()
        try:
            del labels[zone_label]
        except KeyError:
            pass
        time_time = time.time()

        request = PodRequest(
            ts=time_time,
            pod_name=f'{fn}{deployment_pattern}-{pod_name_id}',
            container_id='N/A',
            image=fn_to_spawn.image,
            labels=labels,
            resource_requests=fn_to_spawn.resource_requests,
            namespace=fn_to_spawn.namespace
        )

        scale_schedule_event = ScaleScheduleEvent(
            ts=time_time,
            fn=fn,
            pod=request,
            dest_zone='',
            origin_zone='',
            delete=False
        )

        return scale_schedule_event
