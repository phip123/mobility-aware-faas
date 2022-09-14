import datetime
import logging
import os
import signal
import time
from typing import Dict, List, Optional

from galileocontext.connections import KubernetesClient, RedisClient
from galileocontext.context.cluster import Context
from galileocontext.daemon import DefaultGalileoContextDaemon
from galileocontext.network.static import StaticNetworkService
from galileocontext.pod.api import PodContainer
from schedulescaleopt.daemon import OptDaemon
from schedulescaleopt.opt.osmotic import OsmoticScalerSchedulerOptimizer, OsmoticScalerParameters
from schedulescaleopt.result.k8s import KubernetesResultConsumer
from schedulescaleopt.result.osmotic.rds import RedisOsmoticResultConsumer
from schedulescaleopt.util.pressure import LogisticFunctionParameters

logger = logging.getLogger(__name__)


def handle_sigterm():
    raise KeyboardInterrupt()


def violates_max_cpu_threshold(threshold: float, lookback: int, ctx: Context, pod: PodContainer) -> bool:
    start = datetime.datetime.now() - datetime.timedelta(seconds=lookback)
    cpu = ctx.telemetry_service.get_container_cpu(pod.container_id, start=start)
    # ratio calculation same as kubernetes: used/requested
    return threshold < cpu['milli_cores'].mean() / pod.parsed_resource_requests.get('cpu')


def violates_min_cpu_threshold(threshold: float, lookback: int, ctx: Context, pod: PodContainer) -> bool:
    start = datetime.datetime.now() - datetime.timedelta(seconds=lookback)
    cpu = ctx.telemetry_service.get_container_cpu(pod.container_id, start=start)
    # ratio calculation same as kubernetes: used/requested
    return threshold > cpu['milli_cores'].mean() / pod.parsed_resource_requests.get('cpu')


def read_latency_requirements() -> Dict[str, float]:
    """

    :return: a dictionary that contains for each deployed a function the latency requirement.
    """
    requirements = {}
    for key in os.environ.keys():
        pattern = 'schedule_scale_opt_latency_requirement_'
        if pattern in key:
            fn = key.replace(pattern, '')
            requirements[fn] = float(os.environ[key])
    return requirements


def read_pressures() -> List[str]:
    key = 'schedule_scale_opt_pressures'
    raw_values = os.getenv(key, None)
    if raw_values is None:
        raise ValueError('no pressures specified')
    else:
        parsed = [x.replace(' ', '') for x in raw_values.split(',')]
        return parsed


def read_logistic_function_parameters() -> Optional[LogisticFunctionParameters]:
    try:
        base = 'schedule_scale_opt'
        a = float(os.environ[f'{base}_a'])
        b = float(os.environ[f'{base}_b'])
        c = float(os.environ[f'{base}_c'])
        d = float(os.environ[f'{base}_d'])
        offset = float(os.environ[f'{base}_offset'])
        return LogisticFunctionParameters(a, b, c, d, offset)
    except KeyError:
        return None


def read_percentile_duration() -> int:
    percentile_duration = os.environ.get('schedule_scale_opt_percentile_duration', 90)
    return int(percentile_duration)


def read_target_time_measure():
    return os.environ.get('schedule_scale_opt_target_time_measure')


def read_lookback() -> int:
    return int(os.environ.get('schedule_scale_opt_lookback', 20))


def read_max_containers() -> int:
    return int(os.environ.get('schedule_scale_opt_max_replicas_busy', 20))


def main():
    signal.signal(signal.SIGTERM, handle_sigterm)

    logging.basicConfig(level=logging._nameToLevel[os.environ.get('galileo_context_logging', 'DEBUG')])
    reconcile_interval = int(os.environ.get('schedule_scale_opt_reconcile_interval', 10))
    ctx_daemon = None
    try:
        rds_client = RedisClient.from_env()
        deployment_pattern = "-deployment"
        network_service = StaticNetworkService.from_env()
        ctx_daemon = DefaultGalileoContextDaemon(deployment_pattern, network_service)
        ctx_daemon.start()
        time.sleep(2)
        ctx = ctx_daemon.context
        lookback = read_lookback()
        max_cpu_threshold = float(os.environ.get('schedule_scale_opt_max_cpu_threshold', 0.8))
        min_cpu_threshold = float(os.environ.get('schedule_scale_opt_min_cpu_threshold', 0.2))
        max_threshold = float(os.environ.get('schedule_scale_opt_max_threshold', 0.8))
        min_threshold = float(os.environ.get('schedule_scale_opt_min_threshold', 0.2))

        # latency from experiments are in ms, rtt in s
        thresholds = read_latency_requirements()

        pressures = read_pressures()

        logistic_function_parameters = read_logistic_function_parameters()

        max_containers = read_max_containers()
        percentile_duration = read_percentile_duration()
        target_time_measure = read_target_time_measure()
        optimizer = OsmoticScalerSchedulerOptimizer(OsmoticScalerParameters(
            violates_threshold=(lambda ctx, pod: violates_max_cpu_threshold(max_cpu_threshold, lookback, ctx, pod)),
            violates_min_threshold=(lambda ctx, pod: violates_min_cpu_threshold(min_cpu_threshold, lookback, ctx, pod)),
            max_threshold=max_threshold,
            min_threshold=min_threshold,
            lookback=lookback,
            function_requirements=thresholds,
            max_latency=network_service.max_latency,
            pressures=pressures,
            deployment_pattern=deployment_pattern,
            logistic_function_parameters=logistic_function_parameters,
            percentile_duration=percentile_duration,
            target_time_measure=target_time_measure,
            max_containers=max_containers
        ))
        k8s_client = KubernetesClient.from_env()
        consumers = [RedisOsmoticResultConsumer(rds_client)]
        if os.environ.get('schedule_scale_opt_dry_run', 'false') == 'false':
            k8s_result_consumer = KubernetesResultConsumer(k8s_client.corev1_api, deployment_pattern=deployment_pattern)
            consumers.append(k8s_result_consumer)
        opt_daemon = OptDaemon(ctx, optimizer, consumers,
                               reconcile_interval)
        opt_daemon.run()

    except KeyboardInterrupt:
        logger.info("Received interrupt. Shutting down...")
    finally:
        if ctx_daemon is not None:
            ctx_daemon.stop()


if __name__ == '__main__':
    main()
