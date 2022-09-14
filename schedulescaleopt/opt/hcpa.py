import datetime
import logging
import math
import time
import uuid
from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np
import pandas as pd

from galileocontext.constants import pod_pending, function_label, zone_label
from galileocontext.context.cluster import Context
from galileocontext.deployments.api import Deployment
from schedulescaleopt.opt.api import ScalerSchedulerOptimizer, HpaScalerSchedulerResult, HpaDecision, PodRequest, \
    ScaleScheduleEvent, HcpaDecision, HcpaScalerSchedulerResult


@dataclass
class HorizontalPodOptimizerSpec:
    # default target utilization is 80%
    # ref: https://github.com/kubernetes/community/blob/master/contributors/design-proposals/autoscaling/horizontal-pod-autoscaler.md
    target_deployment: str
    min_replicas: int
    max_replicas: Optional[int]
    lookback: int
    deployment_pattern: str = '-deployment'
    threshold_tolerance: float = 0.1
    target_avg_utilization: float = 80


logger = logging.getLogger(__name__)


class HorizontalClusterPodOptimizer(ScalerSchedulerOptimizer):
    """
    This Optimizer implementation is based on the official default Kubernetes HPA and uses CPU  usage to determine
    the number of Pods.

    Reference: https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/
    """

    def __init__(self, parameters: Dict[str, HorizontalPodOptimizerSpec]):
        """
        :param parameters: HPA Spec per deployment
        """
        self.parameters = parameters

    def run(self, ctx: Context) -> HcpaScalerSchedulerResult:
        """
        Implements a CPU-based scaling approach based on the HPA.

        The implementation considers all running Pods to be ready!

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
        hpa_decisions = []
        scale_schedule_events = []

        for zone in ctx.zone_service.get_zones():
            for deployment in ctx.deployment_service.get_deployments():
                logger.info(f'HCPA scaling for function {deployment.fn_name}')
                spec = self.parameters.get(deployment.fn_name, None)
                if spec is None:
                    continue
                cpu_usages = []
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
                missing_cpu = 0
                for pod in running_pods:
                    now = datetime.datetime.utcnow()
                    lookback_seconds_ago = now - datetime.timedelta(seconds=spec.lookback)
                    cpu = ctx.telemetry_service.get_container_cpu(pod.container_id, lookback_seconds_ago, now)
                    if cpu is None or len(cpu) == 0:
                        missing_cpu += 1
                    else:
                        cpu_usages.append(cpu)
                if len(cpu_usages) > 0:
                    df_running = pd.concat(cpu_usages)
                else:
                    logger.info(f'No CPU usage data for function: {deployment.fn_name}, skip iteration')
                    hpa_decision = HcpaDecision(
                        ts=time.time(),
                        zone=zone,
                        fn=deployment.fn_name,
                        cpu_mean=-1,
                        recalculated_cpu_mean=-1,
                        target_utilization=spec.target_avg_utilization,
                        base_scale_ratio=-1,
                        running_pods=no_of_running_pods,
                        pending_pods=no_of_pending_pods,
                        missing_cpu=missing_cpu,
                        threshold_tolerance=spec.threshold_tolerance,
                        desired_replicas=no_of_pods,
                        recalculated_desired_replicas=-1
                    )
                    hpa_decisions.append(hpa_decision)
                    continue

                # mean utilization over all running pods
                mean_per_pod = df_running.groupby('container_id').mean()
                cpu_mean = mean_per_pod['percentage'].mean()

                base_scale_ratio = cpu_mean / spec.target_avg_utilization
                logger.info(f"Base scale ratio without factoring in missing cpu or not-yet-ready pods is: {base_scale_ratio}."
                            f" With a mean cpu of {cpu_mean} and a target of {spec.target_avg_utilization}")
                if 1 + spec.threshold_tolerance > base_scale_ratio > 1 - spec.threshold_tolerance:
                    logger.info(f'Utilization ({cpu_mean}) was close enough to target ({spec.target_avg_utilization}) '
                                f'with a tolerance of {spec.threshold_tolerance}')
                    hpa_decision = HcpaDecision(
                        ts=time.time(),
                        zone=zone,
                        fn=deployment.fn_name,
                        cpu_mean=cpu_mean,
                        recalculated_cpu_mean=-1,
                        target_utilization=spec.target_avg_utilization,
                        base_scale_ratio=base_scale_ratio,
                        running_pods=no_of_running_pods,
                        pending_pods=no_of_pending_pods,
                        missing_cpu=missing_cpu,
                        threshold_tolerance=spec.threshold_tolerance,
                        desired_replicas=no_of_pods,
                        recalculated_desired_replicas=no_of_pods
                    )
                    hpa_decisions.append(hpa_decision)
                    continue


                desired_replicas = math.ceil(no_of_running_pods * (base_scale_ratio))
                if desired_replicas == no_of_running_pods:
                    logger.info(f"No scaling actions necessary for {deployment.fn_name}")
                    hpa_decision = HcpaDecision(
                        ts=time.time(),
                        zone=zone,
                        fn=deployment.fn_name,
                        cpu_mean=cpu_mean,
                        recalculated_cpu_mean=-1,
                        target_utilization=spec.target_avg_utilization,
                        base_scale_ratio=base_scale_ratio,
                        running_pods=no_of_running_pods,
                        pending_pods=no_of_pending_pods,
                        missing_cpu=missing_cpu,
                        threshold_tolerance=spec.threshold_tolerance,
                        desired_replicas=no_of_pods,
                        recalculated_desired_replicas=no_of_pods
                    )
                    hpa_decisions.append(hpa_decision)
                    continue

                logger.info(f"Scale to {desired_replicas} from {no_of_running_pods} without "
                            f"factoring in not-yet-ready pods")
                if desired_replicas > no_of_running_pods:
                    logger.info("Scale up")
                    recalculated_cpu = mean_per_pod['percentage'].tolist()

                    # add 0% usage for all missing cpu pods
                    for i in range(0, missing_cpu):
                        recalculated_cpu.append(0)

                    # add 0% usage for all pending (not-yet-ready) pods
                    for i in range(0, len(pending_pods)):
                        recalculated_cpu.append(0)


                    # recalculate cpu mean
                    recalculated_cpu_mean = np.mean(recalculated_cpu)

                    # recalculate ratio
                    recalculated_ratio = recalculated_cpu_mean / spec.target_avg_utilization

                    logger.info(
                        f"Recalculated scale ratio without factoring in missing cpu or not-yet-ready pods is: "
                        f"{recalculated_ratio}."
                        f" With a mean cpu of {recalculated_cpu_mean} and a target of {spec.target_avg_utilization}")

                    # recalculate desired replicas
                    recalculated_desired_replicas = math.ceil(no_of_pods * recalculated_ratio)

                    # check if ratio is now within tolerance
                    if 1 + spec.threshold_tolerance > recalculated_ratio > 1 - spec.threshold_tolerance:
                        logger.info(f'Recalculated utilization ({recalculated_cpu_mean}) was close enough to target '
                                    f'({spec.target_avg_utilization}) '
                                    f'with a tolerance of {spec.threshold_tolerance}')
                        hpa_decision = HcpaDecision(
                            ts=time.time(),
                            zone=zone,
                            fn=deployment.fn_name,
                            cpu_mean=cpu_mean,
                            recalculated_cpu_mean=-1,
                            target_utilization=spec.target_avg_utilization,
                            base_scale_ratio=base_scale_ratio,
                            running_pods=no_of_running_pods,
                            pending_pods=no_of_pending_pods,
                            missing_cpu=missing_cpu,
                            threshold_tolerance=spec.threshold_tolerance,
                            desired_replicas=desired_replicas,
                            recalculated_desired_replicas=recalculated_desired_replicas
                        )
                        hpa_decisions.append(hpa_decision)
                        continue

                    # check if action is now reveresed
                    if recalculated_desired_replicas <= no_of_pods:
                        logger.info("Recalculation reversed scaling action and therefore no action happens")
                        hpa_decision = HcpaDecision(
                            ts=time.time(),
                            zone=zone,
                            fn=deployment.fn_name,
                            cpu_mean=cpu_mean,
                            recalculated_cpu_mean=-1,
                            target_utilization=spec.target_avg_utilization,
                            base_scale_ratio=base_scale_ratio,
                            running_pods=no_of_running_pods,
                            pending_pods=no_of_pending_pods,
                            missing_cpu=missing_cpu,
                            threshold_tolerance=spec.threshold_tolerance,
                            desired_replicas=desired_replicas,
                            recalculated_desired_replicas=recalculated_desired_replicas
                        )
                        hpa_decisions.append(hpa_decision)
                        continue

                    # check if new number of pods is over the maximum. if yes => set to minimum
                    if spec.max_replicas is not None and recalculated_desired_replicas > spec.max_replicas:
                        recalculated_desired_replicas = spec.max_replicas

                    scale_up_containers = recalculated_desired_replicas - no_of_pods

                    for i in range(0, scale_up_containers):
                        result = self.prepare_pod_request(deployment, zone, spec.deployment_pattern)
                        scale_schedule_events.append(result)

                else:
                    logger.info("Scale down")
                    recalculated_cpu = mean_per_pod['percentage'].tolist()
                    no_of_pods = no_of_running_pods

                    # add 100% usage for all missing cpu pods
                    for i in range(0, missing_cpu):
                        recalculated_cpu.append(100)

                    # recalculate cpu mean
                    recalculated_cpu_mean = np.mean(recalculated_cpu)
                    if type(recalculated_cpu_mean) is np.ndarray:
                        recalculated_cpu_mean = recalculated_cpu_mean[0]

                    # recalculate ratio
                    recalculated_ratio = recalculated_cpu_mean / spec.target_avg_utilization

                    logger.info(
                        f"Recalculated scale ratio without factoring in missing cpu or not-yet-ready pods is: "
                        f"{recalculated_ratio}."
                        f" With a mean cpu of {recalculated_cpu_mean} and a target of {spec.target_avg_utilization}")

                    # recalculate desired replicas
                    recalculated_desired_replicas = math.ceil(no_of_pods * recalculated_ratio)

                    # check if ratio is now within tolerance
                    if 1 + spec.threshold_tolerance > recalculated_ratio > 1 - spec.threshold_tolerance:
                        logger.info(
                            f'Recalculated utilization ({recalculated_cpu_mean}) was close enough '
                            f'to target ({spec.target_avg_utilization}) '
                            f'with a tolerance of {spec.threshold_tolerance}')
                        hpa_decision = HcpaDecision(
                            ts=time.time(),
                            zone=zone,
                            fn=deployment.fn_name,
                            cpu_mean=cpu_mean,
                            recalculated_cpu_mean=-1,
                            target_utilization=spec.target_avg_utilization,
                            base_scale_ratio=base_scale_ratio,
                            running_pods=no_of_running_pods,
                            pending_pods=no_of_pending_pods,
                            missing_cpu=missing_cpu,
                            threshold_tolerance=spec.threshold_tolerance,
                            desired_replicas=desired_replicas,
                            recalculated_desired_replicas=recalculated_desired_replicas
                        )
                        hpa_decisions.append(hpa_decision)
                        continue

                    # check if action is now reveresed
                    if recalculated_desired_replicas >= no_of_pods:
                        logger.info("Recalculation reversed scaling action and therefore no action happens")
                        hpa_decision = HcpaDecision(
                            ts=time.time(),
                            zone=zone,
                            fn=deployment.fn_name,
                            cpu_mean=cpu_mean,
                            recalculated_cpu_mean=-1,
                            target_utilization=spec.target_avg_utilization,
                            base_scale_ratio=base_scale_ratio,
                            running_pods=no_of_running_pods,
                            pending_pods=no_of_pending_pods,
                            missing_cpu=missing_cpu,
                            threshold_tolerance=spec.threshold_tolerance,
                            desired_replicas=desired_replicas,
                            recalculated_desired_replicas=recalculated_desired_replicas
                        )
                        hpa_decisions.append(hpa_decision)
                        continue

                    # check if new number of pods is below the minimum. if yes => set to minimum
                    if recalculated_desired_replicas < spec.min_replicas:
                        recalculated_desired_replicas = spec.min_replicas

                    scale_down_containers = no_of_pods - recalculated_desired_replicas

                    # choose the last added containers
                    to_remove = running_pods[no_of_running_pods-scale_down_containers:]
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
                            origin_zone=zone,
                            dest_zone=zone,
                            delete=True
                        )

                        scale_schedule_events.append(event)

                hpa_decision = HcpaDecision(
                    ts=time.time(),
                    zone=zone,
                    fn=deployment.fn_name,
                    cpu_mean=cpu_mean,
                    recalculated_cpu_mean=recalculated_cpu_mean,
                    target_utilization=spec.target_avg_utilization,
                    base_scale_ratio=base_scale_ratio,
                    running_pods=no_of_running_pods,
                    pending_pods=no_of_pending_pods,
                    missing_cpu=missing_cpu,
                    threshold_tolerance=spec.threshold_tolerance,
                    desired_replicas=desired_replicas,
                    recalculated_desired_replicas=recalculated_desired_replicas
                )

                hpa_decisions.append(hpa_decision)

        return HcpaScalerSchedulerResult(decisions=hpa_decisions, scale_schedule_events=scale_schedule_events)

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