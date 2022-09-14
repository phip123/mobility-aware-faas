import datetime
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, List, Dict, Tuple, Optional

import numpy as np
import pandas as pd

from galileocontext.constants import pod_type_label, api_gateway_type_label, zone_label, function_label, pod_pending
from galileocontext.context.cluster import Context
from galileocontext.deployments.api import Deployment
from galileocontext.pod.api import PodContainer, default_milli_cpu_request, default_mem_request, parse_cpu_millis
from schedulescaleopt.opt.api import ScalerSchedulerOptimizer, ApiPressureResult, \
    PressureReqResult, PressureDistResult, PressureLatencyRequirementResult, PodRequest, ScaleScheduleEvent, \
    FunctionPressureResult, ClientPressureResult, OsmoticScalerSchedulerResult, CpuUsageResult
from schedulescaleopt.util.pressure import logistic_curve, LogisticFunctionParameters
from schedulescaleopt.util.storage import parse_size_string_to_bytes

logger = logging.getLogger(__name__)


@dataclass
class OsmoticScalerParameters:
    violates_threshold: Callable[[Context, PodContainer], bool]
    violates_min_threshold: Callable[[Context, PodContainer], bool]
    # this thresholds are used to determine functions under pressure
    max_threshold: float
    min_threshold: float
    function_requirements: Dict[str, float]
    max_latency: float
    lookback: int
    pressures: List[str]
    max_containers: int
    # either latency (in ms) or rtt (in s)
    target_time_measure: str
    percentile_duration: float = 90
    deployment_pattern: str = '-deployment'
    logistic_function_parameters: Optional[LogisticFunctionParameters] = None


def get_average_requests_over_replicas(fn: str, traces: pd.DataFrame):
    """
    Calculates the average number of requests over all replicas of one function.
    :param fn: function name
    :return:
    """
    df = traces[traces['function'] == fn]
    return df.groupby('container_id').count()['ts'].mean().mean()


def zone_has_enough_resources(deployment: Deployment, target_zone: str, ctx: Context) -> bool:
    """
    Evaluates if devices in the target zone has a device that can host the given deployment.
    """
    requested_cpu = deployment.parsed_resource_requests.get('cpu', parse_cpu_millis(default_milli_cpu_request))
    requested_mem = deployment.parsed_resource_requests.get('memory', parse_size_string_to_bytes(default_mem_request))
    for node in ctx.node_service.find_nodes_in_zone(target_zone):
        # multiply by 1000 to get milli cpus
        allocatable_cpu = int(node.allocatable['cpu'].replace('m', '')) * 1000
        allocatable_memory = parse_size_string_to_bytes(node.allocatable['memory'])
        pods = ctx.pod_service.get_pod_containers_on_node(node)
        if len(pods) == int(node.allocatable['pods']):
            continue
        for pod in pods:
            pod_resource_requests = pod.parsed_resource_requests
            cpu_request = pod_resource_requests['cpu']
            memory_request = pod_resource_requests['memory']

            allocatable_cpu -= cpu_request
            allocatable_memory -= memory_request

        if allocatable_cpu >= requested_cpu and allocatable_memory > requested_mem:
            return True
    return False


def is_zero_sum_action(create_events: Dict[str, List[str]], x: ScaleScheduleEvent):
    # see if the events have the same destination zone
    to_create = create_events.get(x.dest_zone, None)
    if to_create is not None:

        # check if the function that is supposed to be deleted also in the list of containers to spawn
        if x.fn in to_create:
            return True

    return False


class OsmoticScalerSchedulerOptimizer(ScalerSchedulerOptimizer):

    def __init__(self, parameters: OsmoticScalerParameters):
        self.parameters = parameters

    def remove_zero_sum_actions(self, create_results: OsmoticScalerSchedulerResult,
                                delete_results: OsmoticScalerSchedulerResult):
        create_events = defaultdict(list)
        for result in create_results.scale_schedule_events:
            create_events[result.dest_zone].append(result.fn)

        # get all delete actions that are not reversing the creation event
        filtered_delete = list(
            filter(lambda x: not is_zero_sum_action(create_events, x), delete_results.scale_schedule_events))
        logger.info(f"zero sum actions were identified: {len(filtered_delete) != delete_results.scale_schedule_events}")
        delete_results.scale_schedule_events = filtered_delete
        return delete_results

    def run(self, ctx: Context) -> OsmoticScalerSchedulerResult:
        # TODO probably a generator
        logger.info("start to figure scale up out")
        result = OsmoticScalerSchedulerResult([], [], [], [], [], [], [], [], [], [])
        pressure_values = self.calculate_pressure_per_fn_per_zone(ctx, result)
        if pressure_values is None:
            logger.info("No pressure values calculated, no further execution")
            return result
        delete_results = self.scale_down(pressure_values, ctx)
        # TODO create dictionary by function based on delete to support multi-function experiments
        create_results = self.scale_up(pressure_values, ctx, len(delete_results.scale_schedule_events),
                                       self.parameters.max_containers)
        delete_results = self.remove_zero_sum_actions(create_results, delete_results)
        logger.info(f"figured out scaling, {len(create_results.scale_schedule_events)} up scale events")
        logger.info("figure our scale down")
        logger.info(f"figured out down scaling, {len(delete_results.scale_schedule_events)} down scale events")
        return OsmoticScalerSchedulerResult(
            create_results.api_pressures + delete_results.api_pressures + result.api_pressures,
            create_results.fn_pressures + delete_results.fn_pressures + result.fn_pressures,
            create_results.client_pressures + delete_results.client_pressures + result.client_pressures,
            create_results.pressure_requests + delete_results.pressure_requests + result.pressure_requests,
            create_results.pressure_latency_requirement_results + delete_results.pressure_latency_requirement_results + result.pressure_latency_requirement_results,
            create_results.pressure_dist_results + delete_results.pressure_dist_results + result.pressure_dist_results,
            create_results.pressure_latency_requirement_log_results + delete_results.pressure_latency_requirement_log_results + result.pressure_latency_requirement_log_results,
            create_results.pressure_rtt_log_results + delete_results.pressure_rtt_log_results + result.pressure_rtt_log_results,
            create_results.cpu_usage_results + delete_results.cpu_usage_results + result.cpu_usage_results,
            create_results.scale_schedule_events + delete_results.scale_schedule_events
        )

    def scale_up(self, pressure_values: pd.DataFrame, ctx: Context, teardowns: int,
                 max_replica: int) -> OsmoticScalerSchedulerResult:
        """
        Dataframe contains: 'fn', 'fn_zone', 'client_zone', 'pressure'
        """
        result = OsmoticScalerSchedulerResult([], [], [], [], [], [], [], [], [], [])
        under_pressures = self.is_under_pressure(pressure_values, ctx)
        logger.info(" under pressure_gateway %d", len(under_pressures))
        under_pressure_new = []
        new_pods = {}
        for under_pressure in under_pressures:
            deployment = under_pressure[1]
            running_pods = len(ctx.pod_service.get_pod_containers_of_deployment(deployment.original_name))
            pending_pods = len(ctx.pod_service.get_pod_containers_of_deployment(deployment.original_name, running=False,
                                                                                status=pod_pending))
            all_pods = running_pods + pending_pods
            no_new_pods = new_pods.get(deployment.fn_name, 0)
            if ((all_pods + no_new_pods) - teardowns) < max_replica:
                under_pressure_new.append(under_pressure)
                if new_pods.get(deployment.fn_name, None) is None:
                    new_pods[deployment.fn_name] = 1
                else:
                    new_pods[deployment.fn_name] += 1

        self.scheduling_policy(pressure_values, ctx, under_pressure_new, result)
        return result

    def scale_down(self, pressure_values: pd.DataFrame, ctx: Context) -> OsmoticScalerSchedulerResult:
        """
         Dataframe contains: 'fn', 'fn_zone', 'client_zone', 'pressure'
         """
        result = OsmoticScalerSchedulerResult([], [], [], [], [], [], [], [], [], [])
        # TODO make teardown only aware
        not_under_pressure = self.is_not_under_pressure(pressure_values, ctx)
        logger.info("not under pressure_gateway %d", len(not_under_pressure))
        self.teardown_policy(pressure_values, ctx, not_under_pressure, result)
        return result

    def teardown_policy(
            self,
            pressure_per_zone: pd.DataFrame,
            ctx: Context,
            scale_functions: List[Tuple[PodContainer, Deployment]],
            result: OsmoticScalerSchedulerResult
    ):
        """
        Dataframe contains: 'fn', 'fn_zone', 'client_zone', 'pressure'
        """
        backup = {}
        for event in scale_functions:
            deployment = event[1]
            fn = deployment.labels[function_label]
            all_containers = ctx.pod_service.find_pod_containers_with_labels(
                {function_label: fn})
            backup[fn] = len(all_containers)

        for event in scale_functions:
            gateway = event[0]
            deployment = event[1]
            fn = deployment.labels[function_label]
            gateway_node = ctx.node_service.find(gateway.nodeName)
            zone = gateway_node.labels[zone_label]
            containers = ctx.pod_service.find_pod_containers_with_labels(
                {function_label: fn}, node_labels={zone_label: zone})
            all_containers = ctx.pod_service.find_pod_containers_with_labels(
                {function_label: fn})
            if len(containers) == 0:
                logger.info(
                    f"Wanted to remove container with function {fn} but there is no running container anymore in zone {zone}")
                continue
            if len(all_containers) == 1:
                logger.info(
                    f"Wanted to remove container with function {fn} but there is only one running container anymore in zone {zone}")
                continue
            if backup[fn] <= 1:
                logger.info(f"Tear down policy wanted to scale down function too many times")
                continue
            remove = self.container_with_lowest_resource_usage(containers, ctx)
            if remove is None:
                continue

            backup[fn] -= 1

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
                fn=fn,
                pod=pod_request,
                origin_zone=zone,
                dest_zone=zone,
                delete=True
            )
            result.scale_schedule_events.append(event)

    def is_not_under_pressure(
            self, pressure_values: pd.DataFrame, ctx: Context
    ) -> List[Tuple[PodContainer, Deployment]]:
        """
        Checks for each gateway if its current pressure violates the threshold
        Dataframe contains: 'fn', 'fn_zone', 'client_zone', 'pressure'
        :param gateways: gateways to check the pressure
        :param threshold: pressure threshold
        :return: gateways that are under pressure
        """

        not_under_pressure = []
        # reduce df to look at the mean pressure over all clients per  function and zone
        if len(pressure_values) == 0:
            logger.info("No pressure values")
            return []

        pressure_df = pressure_values.groupby(['fn', 'fn_zone']).mean()
        for gateway in ctx.pod_service.find_pod_containers_with_labels({pod_type_label: api_gateway_type_label}):
            gateway_node = ctx.node_service.find(gateway.nodeName)
            zone = gateway_node.labels[zone_label]
            for deployment in ctx.deployment_service.get_deployments():
                try:
                    mean_pressure = pressure_df.loc[deployment.fn_name].loc[zone]['pressure']
                    if mean_pressure < self.parameters.min_threshold:
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
                        if len(pending_pods) > 0:
                            logger.info(
                                f"Wanted to scale down FN {deployment.fn_name} in zone {zone}, but had pending pods.")
                        else:
                            not_under_pressure.append((gateway, deployment))
                except KeyError:
                    if deployment.labels.get(function_label, None) is not None:
                        logger.info(f'No pressure values found for {zone} - {deployment} - try to shut down')
                        not_under_pressure.append((gateway, deployment))
        return not_under_pressure

    def scheduling_policy(
            self,
            pressure_per_zone: pd.DataFrame,
            ctx: Context,
            scale_functions: List[Tuple[PodContainer, Deployment, PodContainer]],
            result: OsmoticScalerSchedulerResult
    ):
        """
        Dataframe contains: 'fn', 'fn_zone', 'client_zone', 'pressure'
        """
        for event in scale_functions:
            target_gateway = event[0]
            deployment = event[1]
            violation_origin_gateway = event[2]
            target_zone = self.global_scheduling_policy(deployment, target_gateway, pressure_per_zone, ctx)
            violation_origin_gateway_node = ctx.node_service.find(violation_origin_gateway.nodeName)
            origin_zone = violation_origin_gateway_node.labels[zone_label]
            self.prepare_pod_request(deployment, origin_zone=origin_zone, target_zone=target_zone, result=result)

    def prepare_pod_request(self, fn_to_spawn: Deployment, target_zone: str, origin_zone: str,
                            result: OsmoticScalerSchedulerResult):
        pod_name_id = str(uuid.uuid4())[:5]
        fn = fn_to_spawn.labels[function_label]
        labels = fn_to_spawn.labels.copy()
        labels[zone_label] = target_zone

        time_time = time.time()

        request = PodRequest(
            ts=time_time,
            pod_name=f'{fn}{self.parameters.deployment_pattern}-{pod_name_id}',
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
            dest_zone=target_zone,
            origin_zone=origin_zone,
            delete=False
        )

        result.scale_schedule_events.append(scale_schedule_event)

    def global_scheduling_policy(
            self, deployment: Deployment, target_gateway: PodContainer, pressure_values_by_fn_by_zone: pd.DataFrame,
            ctx: Context
    ) -> str:
        """
        Finds the region to place the given deployment in.
        Determines the region based on pressure and latency.
        Finds the nearest region (relative to the gateway's region) with the highest pressure.
        The highest pressure indicates that we need more resources there.
        param: pressure_values_by_fn_by_zone:
        Dataframe contains: 'fn', 'fn_zone', 'client_zone', 'pressure'
        """
        target_gateway_node = ctx.node_service.find(target_gateway.nodeName)
        target_zone = target_gateway_node.labels[zone_label]
        # in case the initial target zone has enough resources, we can schedule it there
        if zone_has_enough_resources(deployment, target_zone, ctx):
            return target_zone
        else:
            # otherwise we have to go through neighboring zones which can host the app
            l = []
            # Make list of pressure values and accompanying latency to afterwards find the zone where the highest pressure
            # is coming from
            for other_zone in ctx.zone_service.get_zones():
                if other_zone == target_zone:
                    continue
                other_gateway = ctx.pod_service.find_pod_containers_with_labels(
                    {pod_type_label: api_gateway_type_label}, node_labels={zone_label: other_zone})[0]
                other_node = other_gateway.nodeName
                node = target_gateway.nodeName
                latency = ctx.network_service.get_latency(other_node, node)
                df = pressure_values_by_fn_by_zone[pressure_values_by_fn_by_zone['fn'] == deployment.fn_name]
                df = df[df['client_zone'] == target_zone]
                df = df[df['fn_zone'] == other_zone]
                if len(df) == 0:
                    # we ignore compute units in this step that don't have a pressure, these units are considered
                    # further down in case no zone fulfills requirements
                    continue
                else:
                    p_c_x_f = df['pressure'].iloc[0]
                l.append((latency, p_c_x_f, other_zone))

            # sort by latency
            a = sorted(l, key=lambda k: (k[0]))
            for t in a:
                p_c_x_f = t[1]
                target_zone = t[2]
                # check if pressure is already violated
                if p_c_x_f < self.parameters.max_threshold:
                    # check if new target has enough resources
                    if zone_has_enough_resources(deployment, target_zone, ctx):
                        return target_zone

            # in case no zone fulfills above requirements, look for nearest that can host
            for t in a:
                target_zone = t[2]
                if zone_has_enough_resources(deployment, target_zone, ctx):
                    return target_zone

            # this error happens in case basically all resources are  used
            raise ValueError(f"No one can host: {deployment.fn_name}")

    def discover_scale_functions(
            self,
            gateways: List[PodContainer],
            violates_threshold: Callable[[Context, PodContainer], bool],
            ctx: Context,
    ) -> List[Tuple[PodContainer, PodContainer]]:
        """
        Filters and returns all deployed functions that need scaling, in case they violate the threshold.
        :param gateways:
        :param violates_threshold:
        :return:
        """
        scale_functions = []
        for gateway in gateways:
            for deployment in ctx.deployment_service.get_deployments():
                for container in ctx.pod_service.get_pod_containers_of_deployment(deployment.original_name):
                    if violates_threshold(ctx, container):
                        scale_functions.append((gateway, container))
        return scale_functions

    def is_under_pressure(self, pressure_per_gateway: pd.DataFrame, ctx: Context) -> List[
        Tuple[PodContainer, Deployment, PodContainer]]:
        """
       Checks for each gateway and deployment if its current pressure violates the threshold
       :param ctx
       :param result
       :return: gateway and deployment tuples that are under pressure
       """
        under_pressure = []
        if len(pressure_per_gateway) == 0:
            logger.info("No pressure values")
            return []
        # reduce df to look at the mean pressure over all clients per  function and zone
        for gateway in ctx.pod_service.find_pod_containers_with_labels({pod_type_label: api_gateway_type_label}):
            # gateway = a
            gateway_node = ctx.node_service.find(gateway.nodeName)
            zone = gateway_node.labels[zone_label]
            for deployment in ctx.deployment_service.get_deployments():
                for client_zone in ctx.zone_service.get_zones():
                    # client zone = x
                    # check if pressure from x on a is too high, if yes -> try to schedule instance in x!
                    try:
                        mean_pressure = pressure_per_gateway.loc[deployment.fn_name].loc[zone].loc[client_zone][
                            'pressure']
                        if mean_pressure > self.parameters.max_threshold:
                            # at this point we know where the origin for the high pressure comes from
                            target_gateway = \
                                ctx.pod_service.find_pod_containers_with_labels(
                                    {pod_type_label: api_gateway_type_label},
                                    node_labels={zone_label: client_zone})[0]
                            under_pressure.append((target_gateway, deployment, gateway))
                    except KeyError:
                        pass
        return under_pressure

    def find_max_pressure_internal_clients(self, gateway: PodContainer, traces: pd.DataFrame, ctx: Context,
                                           result: OsmoticScalerSchedulerResult) -> float:
        gateway_node = ctx.node_service.find(gateway.nodeName)
        internally_spawned_traces = traces[traces['dest_zone'] == gateway_node.labels[zone_label]]
        clients = internally_spawned_traces['client'].unique()
        amax = 0
        for client in clients:
            p = self.max_pressure_for_client(client, traces, gateway, result, ctx)
            if p[0] > amax:
                amax = p[0]
        return amax

    def find_pressures_for_internal_clients(self, gateway: PodContainer, traces: pd.DataFrame, ctx: Context,
                                            result: OsmoticScalerSchedulerResult) -> Optional[pd.DataFrame]:
        """
        Dataframe contains: 'fn', 'fn_zone', 'client_zone', 'pressure'
        """
        gateway_node = ctx.node_service.find(gateway.nodeName)
        zone = gateway_node.labels[zone_label]
        internally_spawned_traces = traces[traces['dest_zone'] == zone]
        internally_spawned_traces = internally_spawned_traces[internally_spawned_traces['origin_zone'] == zone]
        clients = internally_spawned_traces['client'].unique()

        dfs = []
        for client in clients:
            for_client = self.pressures_for_client(client, zone, traces, gateway, result, ctx)
            if for_client is not None:
                dfs.append(for_client)
            break

        if len(dfs) == 0:
            return None
        df = pd.concat(dfs)
        df['client'] = f'gateway-{zone}'
        return df

    def find_max_pressure_external_clients(self, gateway: PodContainer, traces: pd.DataFrame,
                                           result: OsmoticScalerSchedulerResult, ctx: Context) -> List[
        Tuple[float, PodContainer]]:
        if len(traces) == 0:
            return []
        maxs = []
        gateway_node = ctx.node_service.find(gateway.nodeName)
        external_clients = traces[traces['origin_zone'] != gateway_node.labels[zone_label]][
            ['client', 'origin_zone']]
        if len(external_clients) == 0:
            return []
        unique_externals = external_clients.value_counts()
        for row in unique_externals.keys():
            client = row[0]
            origin_zone = row[1]
            containers = ctx.pod_service.find_pod_containers_with_labels(
                {pod_type_label: api_gateway_type_label}, node_labels={zone_label: origin_zone})
            max_pressure_for_client = self.max_pressure_for_client(client, traces, gateway, result, ctx)
            maxs.append((max_pressure_for_client[0], containers[0]))
        return maxs

    def calculate_pressures_external_clients(self, gateway: PodContainer, traces: pd.DataFrame,
                                             result: OsmoticScalerSchedulerResult, ctx: Context) -> Optional[
        pd.DataFrame]:
        data = {
            'pressure': [],
            'client': [],
            'client_zone': [],
            'fn': [],
            'fn_zone': []
        }
        pressure_values = []
        gateway_node = ctx.node_service.find(gateway.nodeName)
        gateway_zone = gateway_node.labels[zone_label]
        external_clients = traces[traces['origin_zone'] != gateway_zone][
            ['client', 'origin_zone']]
        seen_zones = set()
        unique_externals = external_clients.value_counts()
        # iterate over all external clients but only calculate the pressure once for each zone
        #
        for row in unique_externals.keys():
            client = row[0]
            origin_zone = row[1]
            if origin_zone not in seen_zones:
                pressures = self.pressures_for_client(client, origin_zone, traces, gateway, result, ctx)
                if pressures is None:
                    continue
                pressures['client'] = f'gateway-{gateway_zone}'
                pressure_values.append(pressures)
                seen_zones.add(origin_zone)

        if len(pressure_values) > 0:
            df = pd.concat(pressure_values)
            return df
        else:
            return None

    def max_pressure_for_client(self, client: str, traces: pd.DataFrame, gateway: PodContainer,
                                result: OsmoticScalerSchedulerResult, ctx: Context) -> (float, str, str):
        pressures = self.pressures_for_client(client, traces, gateway, result, ctx)
        amax = (0, None, None)
        for fn, p in pressures.items():
            if amax[0] < p:
                amax = (p, client, fn)

            return amax

    def pressures_for_client(self, client: str, client_zone: str, traces: pd.DataFrame, gateway: PodContainer,
                             result: OsmoticScalerSchedulerResult, ctx: Context) -> Optional[pd.DataFrame]:
        """
        Dataframe contains: 'fn', 'fn_zone', 'client_zone', 'pressure'
        """
        data = defaultdict(list)
        gateway_node = ctx.node_service.find(gateway.nodeName)
        zone = gateway_node.labels[zone_label]
        deployments = ctx.deployment_service.get_deployments()
        for deployment in deployments:
            fn_name = deployment.fn_name
            pods = ctx.pod_service.get_pod_containers_of_deployment(deployment.original_name)
            if len(pods) == 0:
                continue
            pod = pods[0]
            if pod.labels.get(function_label, None) is None:
                continue
            p = self.pressure_by_client_on_function(
                client,
                fn_name,
                self.parameters.function_requirements[fn_name],
                traces,
                ctx,
                gateway,
                result
            )
            data['pressure'].append(p)
            data['client'].append(client)
            data['client_zone'].append(client_zone)
            data['fn'].append(fn_name)
            data['fn_zone'].append(zone)
            result.client_pressures.append(ClientPressureResult(time.time(), client, client_zone, fn_name, zone, p))

        if len(data) > 0:
            return pd.DataFrame(data=data)
        else:
            return None

    def pressure_rtt_log(self, client: str, fn: str, latency_requirement: float, gateway: PodContainer,
                         ctx: Context,
                         result: OsmoticScalerSchedulerResult):
        a = self.parameters.logistic_function_parameters.a
        b = self.parameters.logistic_function_parameters.b
        c = self.parameters.logistic_function_parameters.c
        d = self.parameters.logistic_function_parameters.d
        offset = self.parameters.logistic_function_parameters.offset

        pod_name = client[:27]
        client_node_name = ctx.pod_service.get_pod_container_by_name(pod_name).nodeName
        client_node = ctx.node_service.find(client_node_name)
        client_gateway_node = \
            ctx.pod_service.find_pod_containers_with_labels({pod_type_label: api_gateway_type_label}, node_labels={
                zone_label: client_node.labels[zone_label]})[0]
        client_gateway_node_name = client_gateway_node.nodeName
        client_gateway_zone = client_node.labels[zone_label]

        gateway_node = ctx.node_service.find(gateway.nodeName)
        gateway_zone = gateway_node.labels[zone_label]

        now = datetime.datetime.now()
        lookback_seconds_ago = now - datetime.timedelta(seconds=self.parameters.lookback)
        traces = ctx.trace_service.get_traces_for_fn(fn, lookback_seconds_ago, now, gateway_zone)

        if len(traces) > 0:
            # filter for client zone
            traces = traces[traces['origin_zone'] == client_gateway_zone]

            # percentile of rtt over all traces
            duration_agg = np.percentile(q=self.parameters.percentile_duration,
                                         a=traces[self.parameters.target_time_measure])
            if self.parameters.target_time_measure == 'rtt':
                # convert to ms
                duration_agg *= 1000

            client = f'gateway-{client_gateway_zone}'

            d_moved = d - (d * offset)

            p_lat_a_x_f = logistic_curve(duration_agg, a, b, c, d_moved) / a
        else:
            p_lat_a_x_f = 0
            duration_agg = 0

        result.pressure_rtt_log_results.append(PressureLatencyRequirementResult(
            ts=time.time(),
            value=p_lat_a_x_f,
            distance=duration_agg,
            fn=fn,
            client=client,
            required_latency=latency_requirement,
            gateway=gateway.nodeName,
            max_value=-1,
            scaled_value=duration_agg,
            zone=gateway_zone,
            type='rtt'
        ))
        return p_lat_a_x_f

    def pressure_latency_req_log(self, client: str, fn: str, latency_requirement: float, gateway: PodContainer,
                                 ctx: Context,
                                 result: OsmoticScalerSchedulerResult):
        a = self.parameters.logistic_function_parameters.a
        b = self.parameters.logistic_function_parameters.b
        c = self.parameters.logistic_function_parameters.c
        offset = self.parameters.logistic_function_parameters.offset

        pod_name = client[:27]
        client_node_name = ctx.pod_service.get_pod_container_by_name(pod_name).nodeName
        client_node = ctx.node_service.find(client_node_name)
        client_gateway_node = \
            ctx.pod_service.find_pod_containers_with_labels({pod_type_label: api_gateway_type_label}, node_labels={
                zone_label: client_node.labels[zone_label]})[0]
        client_gateway_node_name = client_gateway_node.nodeName
        client_gateway_zone = client_node.labels[zone_label]

        if client_gateway_node_name == gateway.nodeName:
            # in case both are in the same -> then use average internal distance

            d_x_a = self.calculate_average_internal_distance(gateway, ctx)
        else:
            # otherwise set client gateway as client
            d_x_a = ctx.network_service.get_latency(client_gateway_node_name,
                                                    gateway.nodeName)

        client = f'gateway-{client_gateway_zone}'

        d = latency_requirement - (latency_requirement * offset)

        p_lat_a_x_f = logistic_curve(d_x_a, a, b, c, d) / a

        gateway_node = ctx.node_service.find(gateway.nodeName)
        result.pressure_latency_requirement_log_results.append(PressureLatencyRequirementResult(
            ts=time.time(),
            value=p_lat_a_x_f,
            distance=d_x_a,
            fn=fn,
            client=client,
            required_latency=latency_requirement,
            gateway=gateway.nodeName,
            max_value=-1,
            scaled_value=-1,
            zone=gateway_node.labels[zone_label],
            type='network_latency'
        ))
        return p_lat_a_x_f

    def pressure_cpu_usage(self, fn: str, gateway: PodContainer,
                           ctx: Context,
                           result: OsmoticScalerSchedulerResult) -> float:
        gateway_node = ctx.node_service.find(gateway.nodeName)
        zone = gateway_node.labels[zone_label]
        cpu_usages = []
        running_pods = ctx.pod_service.find_pod_containers_with_labels(
            labels={
                function_label: fn,
            },
            node_labels={
                zone_label: zone
            }
        )
        ts = time.time()
        for pod in running_pods:
            now = datetime.datetime.utcnow()
            lookback_seconds_ago = now - datetime.timedelta(seconds=self.parameters.lookback)
            cpu = ctx.telemetry_service.get_container_cpu(pod.container_id, lookback_seconds_ago, now)
            if cpu is None or len(cpu) == 0:
                pass
            else:
                cpu_usages.append(cpu)
        if len(cpu_usages) > 0:
            df_running = pd.concat(cpu_usages)
        else:
            logger.info(f"no cpu usage values for zone {zone}")
            result.cpu_usage_results.append(CpuUsageResult(ts, fn, len(running_pods), 0, zone))
            return 0

        # mean utilization over all running pods
        mean_per_pod = df_running.groupby('container_id').mean()
        # this utilization is relative to the requested resources -> pressure of cpu usage in contrast to request
        # if 1 => fully utilized
        cpu_mean = mean_per_pod['percentage'].mean() / 100
        result.cpu_usage_results.append(CpuUsageResult(ts, fn, len(running_pods), cpu_mean, zone))
        return cpu_mean

    def pressure_by_client_on_function(
            self,
            client: str,
            function: str,
            required_latency: float,
            traces: pd.DataFrame,
            ctx: Context,
            gateway: PodContainer,
            result: OsmoticScalerSchedulerResult
    ) -> float:
        """
        Dataframe contains: 'fn', 'fn_zone', 'client_zone', 'pressure'
        """

        pressures = {
            'rtt_log': self.pressure_rtt_log(client, function, required_latency, gateway, ctx, result),
            'request': self.pressure_by_num_requests_for_gatewawy(client, function, traces, gateway, result, ctx),
            'network_distance': self.pressure_by_network_distance(client, ctx.network_service.get_max_latency(), ctx,
                                                                  gateway, result),
            'latency_req_unbound': self.pressure_latency_fulfillment(client, required_latency,
                                                                     ctx.network_service.get_max_latency(),
                                                                     ctx, gateway, function,
                                                                     result),
            'latency_req_log': self.pressure_latency_req_log(client, function, required_latency, gateway, ctx, result),
            'cpu_usage': self.pressure_cpu_usage(function, gateway, ctx, result)
        }

        val = 1
        for pressure in self.parameters.pressures:
            val *= pressures[pressure]

        return val

    def pressure_latency_fulfillment(
            self, client: str, required_latency: float, max_latency: float, ctx: Context, gateway: PodContainer,
            fn: str,
            result: OsmoticScalerSchedulerResult
    ) -> float:
        """
        Calculates the pressure based on the fulfillment of the latency requirement.
        I.e., applications have different requirements and therefore the pressure must reflect this circumstance
        :param client: the client that calls
        :param required_latency: the required latency of the function
        :param max_latency: the max latency encountered in the whole network
        :param ctx:
        :param gateway:
        :param result:
        :return: Between 0 and 1 scaled pressure, the higher the value the higher the requirement violation
        """
        pod_name = client[:27]
        client_node_name = ctx.pod_service.get_pod_container_by_name(pod_name).nodeName
        client_node = ctx.node_service.find(client_node_name)
        client_gateway_node = \
            ctx.pod_service.find_pod_containers_with_labels({pod_type_label: api_gateway_type_label}, node_labels={
                zone_label: client_node.labels[zone_label]})[0]
        client_gateway_node_name = client_gateway_node.nodeName
        client_gateway_zone = client_node.labels[zone_label]

        if client_gateway_node_name == gateway.nodeName:
            # in case both are in the same -> then use average internal distance

            d_x_a = self.calculate_average_internal_distance(gateway, ctx)
        else:
            # otherwise set client gateway as client
            d_x_a = ctx.network_service.get_latency(client_gateway_node_name,
                                                    gateway.nodeName)

        client = f'gateway-{client_gateway_zone}'

        v_a_x_f = d_x_a / required_latency
        # if 1 <= v_a_x_f:
        #     p_lat_a_x_f = 1
        # else:
        #     p_lat_a_x_f = v_a_x_f
        p_lat_a_x_f = v_a_x_f
        gateway_node = ctx.node_service.find(gateway.nodeName)
        result.pressure_latency_requirement_results.append(PressureLatencyRequirementResult(
            ts=time.time(),
            value=p_lat_a_x_f,
            distance=d_x_a,
            client=client,
            required_latency=required_latency,
            gateway=gateway.nodeName,
            max_value=-1,
            scaled_value=-1,
            zone=gateway_node.labels[zone_label],
            fn=fn,
            type='network_latency'
        ))
        return p_lat_a_x_f

    def pressure_by_network_distance(self, client: str, max_latency: float, ctx: Context, gateway: PodContainer,
                                     result: OsmoticScalerSchedulerResult) -> float:
        """
        Calculates the pressure by network distance.
        Intuitively, the pressure represents how much more feasible it is to call the function inside the region.
        I.e., consider a latency between client and node of 50 ms, and an average latency of 10ms.
        Hosting the application inside the factory may therefore reduce the latency 5 times.
        Before returning, the value is normalized with respect to the maximum latency in the network (i.e.,
        Australia - Europe)
        :param client: the client which called the function
        :param max_latency: the maximum latency in the whole network
        :return: pressure by network distance
        """
        pod_name = client[:27]
        client_node_name = ctx.pod_service.get_pod_container_by_name(pod_name).nodeName
        client_node = ctx.node_service.find(client_node_name)
        client_gateway_node = \
            ctx.pod_service.find_pod_containers_with_labels({pod_type_label: api_gateway_type_label}, node_labels={
                zone_label: client_node.labels[zone_label]})[0]
        client_gateway_node_name = client_gateway_node.nodeName
        client_gateway_zone = client_node.labels[zone_label]
        avg_d = self.calculate_average_internal_distance(gateway, ctx)

        if client_gateway_node_name == gateway.nodeName:
            # in case both are in the same -> then use average internal distance
            d_x_a = avg_d
        else:
            # otherwise set client gateway as client
            # use distance between the two gateways
            d_x_a = ctx.network_service.get_latency(client_gateway_node_name,
                                                    gateway.nodeName)
        client = f'gateway-{client_gateway_zone}'

        value = d_x_a / max_latency
        gateway_node = ctx.node_service.find(gateway.nodeName)
        result.pressure_dist_results.append(PressureDistResult(
            ts=time.time(),
            value=value,
            distance=d_x_a,
            client=client,
            avg_distance=avg_d,
            gateway=gateway.nodeName,
            max_value=max_latency,
            scaled_value=-1,
            zone=gateway_node.labels[zone_label]
        ))

        return value

    def calculate_average_internal_distance(self, gateway: PodContainer, ctx: Context) -> float:
        gateway_node = ctx.node_service.find(gateway.nodeName)
        zone = gateway_node.labels[zone_label]
        nodes = ctx.node_service.find_nodes_in_zone(zone)
        distances = list(
            map(lambda c: ctx.network_service.get_latency(gateway.nodeName, c.name), nodes)
        )
        return np.mean(distances)

    def pressure_by_num_requests(
            self, client: str, fn: str, traces: pd.DataFrame, gateway: PodContainer,
            result: OsmoticScalerSchedulerResult, ctx: Context
    ) -> float:
        """
        Calculates the pressure based on the number of requests that were served by the gateway.
        We consider only requests from the given function.
        This will yield higher pressure values for the gateway in case the client is spawning more requests than others.
        Dataframe contains: 'fn', 'fn_zone', 'client_zone', 'pressure'
        :param client:
        :param fn:
        :param traces:
        :param gateway:
        :param result:
        :param ctx:
        :return:
        """
        try:
            df = traces[traces['function'] == fn]
            if len(df) == 0:
                return 0

            # traces are already filtered for gateway
            R_a_f = len(df)
            client_traces = df[df['client'] == client]
            R_a_x_f = len(client_traces)
            if R_a_x_f == 0:
                return 0

            client_zone = client_traces['origin_zone'].iloc[0]
            gateway_node = ctx.node_service.find(gateway.nodeName)
            gateway_zone = gateway_node.labels[zone_label]
            unique_client_zones = len(df['origin_zone'].unique())

            n_clients = len(df['client'].unique())
            client_zone_traces = df[df['origin_zone'] == client_zone]
            n_clients_zone = len(client_zone_traces['client'].unique())

            # this pressure suffers from the fact that regions with more clients split the
            # requests and the mean will go lower as the number of clients increase
            # p_req_a_x_f = R_a_x_f / R_a_f <- not usable

            # the new calculation combines the number of requests to the number of average requests
            # possible considering all clients and takes into account how many clients are from that
            # particular region.
            # This should lead to a high pressure for regions which spawn a lot of clients that spawn
            # on average a lot of requests
            utilization = R_a_f / unique_client_zones

            weight = R_a_x_f

            value = weight / utilization

            result.pressure_requests.append(PressureReqResult(
                ts=time.time(),
                value=value,
                weight=weight,
                utilization=utilization,
                client=client,
                fn=fn,
                gateway=gateway.nodeName,
                zone=gateway_zone
            ))

            return value
        except KeyError:
            logger.error(
                f"Wanted to access not called function {fn} to calculate num_request pressure of client {client}"
            )
            return 0

    def pressure_by_num_requests_for_gatewawy(
            self, client: str, fn: str, traces: pd.DataFrame, gateway: PodContainer,
            result: OsmoticScalerSchedulerResult, ctx: Context
    ) -> float:
        """
        Calculates the pressure based on the number of requests that were served by the gateway.
        We consider only requests from the given function.
        This will yield higher pressure values for the gateway in case the client is spawning more requests than others.
        Dataframe contains: 'fn', 'fn_zone', 'client_zone', 'pressure'
        :param client:
        :param fn:
        :param traces:
        :param gateway:
        :param result:
        :param ctx:
        :return:
        """
        try:
            gateway_node = ctx.node_service.find(gateway.nodeName)
            gateway_zone = gateway_node.labels[zone_label]
            df = traces[traces['function'] == fn]
            if len(df) == 0:
                weight = 0
                util = 0
                value = 0
            else:
                # traces are already filtered for gateway
                R_a_f = len(df)
                unique_client_zones = len(df['origin_zone'].unique())
                client_traces = df[df['client'] == client]
                client_zone = client_traces['origin_zone'].iloc[0]
                client = f'gateway-{client_zone}'
                client_zone_traces = df[df['origin_zone'] == client_zone]

                R_a_x_f = len(client_zone_traces)
                if R_a_x_f == 0:
                    weight = 0
                    util = 0
                    value = 0
                else:
                    util = (R_a_f / unique_client_zones)

                    value_max = 0
                    for zone in df['origin_zone'].unique():
                        zone_traces = df[df['origin_zone'] == zone]
                        R_tmp = len(zone_traces)
                        value_tmp = R_tmp / util
                        if value_max < value_tmp:
                            value_max = value_tmp

                    weight = R_a_x_f
                    value = weight / util
                    value = value / value_max

            result.pressure_requests.append(PressureReqResult(
                ts=time.time(),
                value=value,
                weight=weight,
                utilization=util,
                client=client,
                fn=fn,
                gateway=gateway.nodeName,
                zone=gateway_zone
            ))

            return value
        except KeyError:
            logger.error(
                f"Wanted to access not called function {fn} to calculate num_request pressure of client {client}"
            )
            return 0

    def calculate_pressure_per_fn_per_zone(self, ctx: Context, result: OsmoticScalerSchedulerResult) -> Optional[
        pd.DataFrame]:
        dfs = []
        gateways = ctx.pod_service.find_pod_containers_with_labels({pod_type_label: api_gateway_type_label})
        for gateway in gateways:
            fn = self.calculate_pressure_per_fn(gateway, ctx, result)
            if fn is not None:
                dfs.append(fn)
        if len(dfs) == 0:
            return None

        return pd.concat(dfs)

    def calculate_pressure_per_fn(self, gateway: PodContainer, ctx: Context,
                                  result: OsmoticScalerSchedulerResult) -> Optional[pd.DataFrame]:
        """
        Results contains for each deployed function in the region the pressure value (grouped by client zones)
        Dataframe contains: 'fn', 'fn_zone', 'client_zone', 'pressure'
        """
        traces = ctx.trace_service.get_traces_api_gateway(gateway.nodeName)
        gateway_node = ctx.node_service.find(gateway.nodeName)
        zone = gateway_node.labels[zone_label]
        data = defaultdict(list)
        if len(traces) == 0:
            logger.info(f'Found no traces for gateway on node {gateway.nodeName}')
            return None
        pressure_values = []

        internal_pressure_values = self.find_pressures_for_internal_clients(gateway, traces, ctx, result)
        if internal_pressure_values is not None and len(internal_pressure_values) > 0:
            pressure_values.append(internal_pressure_values)

        external_pressure_values = self.calculate_pressures_external_clients(gateway, traces, result, ctx)
        if external_pressure_values is not None and len(external_pressure_values) > 0:
            pressure_values.append(external_pressure_values)

        if (internal_pressure_values is None or len(internal_pressure_values) == 0) and (
                external_pressure_values is None or len(external_pressure_values) == 0):
            logger.info(f'No pressure values calculated for zone {zone}')
            return None

        pressure_values = pd.concat(pressure_values)
        deployments = ctx.deployment_service.get_deployments()
        for deployment in deployments:
            fn_name = deployment.fn_name
            df = pressure_values[pressure_values['fn'] == fn_name]
            for client_zone in df['client_zone'].unique():
                df_client_zone = df[df['client_zone'] == client_zone]
                avg = df_client_zone['pressure'].mean()
                median = df_client_zone['pressure'].median()
                std = df_client_zone['pressure'].std()
                amin = df_client_zone['pressure'].min()
                amax = df_client_zone['pressure'].max()

                data['fn'].append(fn_name)
                data['fn_zone'].append(zone)
                data['client_zone'].append(client_zone)
                data['pressure_avg'].append(avg)
                data['pressure_median'].append(median)
                data['pressure_std'].append(std)
                data['pressure_min'].append(amin)
                data['pressure_max'].append(amax)
                result.fn_pressures.append(
                    FunctionPressureResult(
                        time.time(),
                        fn_name,
                        zone,
                        client_zone,
                        avg,
                        std,
                        amin,
                        amax
                    )
                )
        gateway_pressure = pd.DataFrame(data=data)
        mean_pressure = gateway_pressure['pressure_avg'].mean()
        median_pressure = gateway_pressure['pressure_median'].mean()
        mean_std = gateway_pressure['pressure_std'].mean()
        mean_min = gateway_pressure['pressure_min'].mean()
        mean_max = gateway_pressure['pressure_max'].mean()
        logger.info(f"Avg Pressure gateway ({gateway.nodeName}) over all deployments: {mean_pressure}")
        gateway_node = ctx.node_service.find(gateway.nodeName)
        result.api_pressures.append(
            ApiPressureResult(time.time(), gateway.nodeName, gateway_node.labels[zone_label], mean=mean_pressure,
                              median=median_pressure, std=mean_std,
                              min=mean_min, max=mean_max)
        )
        return pressure_values.groupby(['fn', 'fn_zone', 'client_zone']).mean()

    def container_with_lowest_resource_usage(self, containers: List[PodContainer], ctx: Context) -> Optional[
        PodContainer]:
        cpus = []
        lookback = self.parameters.lookback
        for container in containers:
            start = datetime.datetime.now() - datetime.timedelta(seconds=lookback)
            end = datetime.datetime.now()
            try:
                cpu = ctx.telemetry_service.get_container_cpu(container.container_id, start, end)['percentage'].mean()
                cpus.append((container, cpu))
            except TypeError as e:
                logger.error(e)

        cpus.sort(key=lambda x: x[1])
        return None if len(cpus) == 0 else cpus[0][0]
