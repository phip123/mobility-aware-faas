import datetime
import logging
import math
import time
import uuid
from typing import Dict

import numpy as np

from galileocontext.constants import pod_pending, function_label, zone_label
from galileocontext.context.cluster import Context
from galileocontext.deployments.api import Deployment
from galileocontext.pod.api import PodContainer
from schedulescaleopt.opt.api import ScalerSchedulerOptimizer, PodRequest, \
    ScaleScheduleEvent, HlcpaDecision, HlcpaScalerSchedulerResult
from schedulescaleopt.opt.hlpa import HorizontalLatencyPodOptimizerSpec

logger = logging.getLogger(__name__)


class HorizontalLatencyClusterPodOptimizer(ScalerSchedulerOptimizer):
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

    def run(self, ctx: Context) -> HlcpaScalerSchedulerResult:
        """
        Implements a time-based scaling approach based on the HPA.
        The time measure can be configured via the 'HorizontalLatencyPodOptimizerSpec'.
        Available time measures are: 'latency' & 'rtt'.
        Whereas latency only represents the network latency between the gateway and the final pod.
        And rtt also includes the processing time, i.e., the whole request.

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
        hlcpa_decisions = []
        scale_schedule_events = []


        for deployment in ctx.deployment_service.get_deployments():
            all_running_pods = ctx.pod_service.find_pod_containers_with_labels(
                labels={
                    function_label: deployment.fn_name,
                }
            )
            all_pending_pods = ctx.pod_service.find_pod_containers_with_labels(
                labels={
                    function_label: deployment.fn_name,
                },
                running=False,
                status=pod_pending
            )
            no_all_pods = len(all_pending_pods + all_running_pods)

            updated_no_of_pods = no_all_pods

            for zone in ctx.zone_service.get_zones():
                logger.info(f'HLCPA scaling for function {deployment.fn_name}')
                spec = self.parameters.get(deployment.fn_name, None)
                if spec is None:
                    continue

                running_pods = ctx.pod_service.find_pod_containers_with_labels(
                    labels={
                        function_label: deployment.fn_name,
                    },
                    node_labels={
                        zone_label: zone
                    }
                )
                pending_pods = ctx.pod_service.find_pod_containers_with_labels(
                    labels={
                        function_label: deployment.fn_name,
                    },
                    node_labels={
                        zone_label: zone
                    },
                    running=False,
                    status=pod_pending
                )

                no_of_running_pods = len(running_pods)
                no_of_pending_pods = len(pending_pods)
                no_of_pods = no_of_running_pods + no_of_pending_pods
                now = datetime.datetime.now()
                lookback_seconds_ago = now - datetime.timedelta(seconds=spec.lookback)
                traces = ctx.trace_service.get_traces_for_deployment(deployment, lookback_seconds_ago, now, zone)
                if len(traces) == 0:
                    desired_replicas = max(1, int(no_of_running_pods / 2))
                    if desired_replicas == 1 and no_of_running_pods == 1:
                        scale_down = 0
                    else:
                        scale_down = no_of_running_pods - desired_replicas
                    logger.info(f'No trace data for function: {deployment.fn_name}, scale down by half, from {no_of_running_pods} to {desired_replicas}')
                    hlcpa_decision = HlcpaDecision(
                        ts=time.time(),
                        zone=zone,
                        fn=deployment.fn_name,
                        duration_agg=0,
                        target_duration=spec.target_duration,
                        percentile=spec.percentile_duration,
                        base_scale_ratio=-1,
                        running_pods=no_of_running_pods,
                        pending_pods=no_of_pending_pods,
                        threshold_tolerance=spec.threshold_tolerance,
                        desired_replicas=desired_replicas,
                    )

                    hlcpa_decisions.append(hlcpa_decision)

                    to_remove = running_pods[no_of_running_pods - scale_down:]
                    for i in range(0, scale_down):
                        remove = to_remove[i]
                        event = self.prepare_scale_down(deployment, remove)
                        scale_schedule_events.append(event)
                        updated_no_of_pods -= 1
                    continue

                if spec.target_time_measure == 'rtt':
                    traces[spec.target_time_measure] = traces[spec.target_time_measure] * 1000

                # percentile of rtt over all traces
                duration_agg = np.percentile(q=spec.percentile_duration, a=traces[spec.target_time_measure])

                base_scale_ratio = duration_agg / spec.target_duration
                logger.info(
                    f"Base scale ratio for zone {zone} is : {base_scale_ratio}."
                    f" With a {spec.percentile_duration} percentile of {duration_agg} and a target of {spec.target_duration}")
                if 1 + spec.threshold_tolerance > base_scale_ratio > 1 - spec.threshold_tolerance:
                    logger.info(
                        f'{spec.target_time_measure} percentile ({duration_agg}) was close enough to target ({spec.target_duration}) '
                        f'with a tolerance of {spec.threshold_tolerance}')
                    hlcpa_decision = HlcpaDecision(
                        ts=time.time(),
                        zone=zone,
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
                    hlcpa_decisions.append(hlcpa_decision)
                    continue

                desired_replicas = math.ceil(no_of_running_pods * (base_scale_ratio))
                if desired_replicas == no_of_running_pods:
                    logger.info(f"No scaling actions necessary for {deployment.fn_name}")
                    continue
                logger.info(f"Scale to {desired_replicas} from {no_of_running_pods} in zone {zone}")
                if desired_replicas > no_of_running_pods:
                    added_replicas = desired_replicas - no_of_running_pods

                    # check if new number of pods is over the maximum. if yes => set to minimum
                    if (spec.max_replicas is not None and desired_replicas > spec.max_replicas):
                        recalculated_desired_replicas = spec.max_replicas
                    elif updated_no_of_pods + added_replicas > spec.max_replicas:
                        # check if adding new pods would violate global maximum limit
                        recalculated_desired_replicas = (spec.max_replicas - updated_no_of_pods) + no_of_pods
                    else:
                        recalculated_desired_replicas = desired_replicas

                    scale_up_containers = recalculated_desired_replicas - no_of_pods

                    for i in range(0, scale_up_containers):
                        updated_no_of_pods += 1
                        result = self.prepare_pod_request(deployment, zone, spec.deployment_pattern)
                        scale_schedule_events.append(result)

                else:
                    logger.info("Scale down")
                    scale_down_containers = no_of_pods - desired_replicas

                    # choose the last added containers
                    to_remove = running_pods[no_of_running_pods - scale_down_containers:]
                    for i in range(0, scale_down_containers):
                        updated_no_of_pods -= 1
                        remove = to_remove[i]
                        event = self.prepare_scale_down(deployment, remove)

                        scale_schedule_events.append(event)

                hlcpa_decision = HlcpaDecision(
                    ts=time.time(),
                    fn=deployment.fn_name,
                    zone=zone,
                    duration_agg=duration_agg,
                    target_duration=spec.target_duration,
                    base_scale_ratio=base_scale_ratio,
                    running_pods=no_of_running_pods,
                    pending_pods=no_of_pending_pods,
                    threshold_tolerance=spec.threshold_tolerance,
                    desired_replicas=desired_replicas,
                    percentile=spec.percentile_duration
                )

                hlcpa_decisions.append(hlcpa_decision)

        return HlcpaScalerSchedulerResult(decisions=hlcpa_decisions, scale_schedule_events=scale_schedule_events)

    def prepare_scale_down(self, deployment: Deployment, remove: PodContainer):
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
        return event

    def prepare_pod_request(self, fn_to_spawn: Deployment, zone: str, deployment_pattern: str) -> ScaleScheduleEvent:
        pod_name_id = str(uuid.uuid4())[:5]
        fn = fn_to_spawn.labels[function_label]
        labels = fn_to_spawn.labels.copy()
        labels[zone_label] = zone
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
            dest_zone=zone,
            origin_zone=zone,
            delete=False
        )

        return scale_schedule_event
