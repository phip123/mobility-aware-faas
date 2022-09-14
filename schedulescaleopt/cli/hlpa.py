import logging
import os
import signal
import time
from typing import Dict

from galileocontext.connections import KubernetesClient, RedisClient
from galileocontext.daemon import DefaultGalileoContextDaemon
from galileocontext.network.static import StaticNetworkService
from schedulescaleopt.daemon import OptDaemon
from schedulescaleopt.opt.hlpa import HorizontalLatencyPodOptimizerSpec, HorizontalLatencyPodOptimizer
from schedulescaleopt.result.hlpa.rds import RedisHlpaResultConsumer
from schedulescaleopt.result.hpa.rds import RedisHpaResultConsumer
from schedulescaleopt.result.k8s import KubernetesResultConsumer

logger = logging.getLogger(__name__)


def handle_sigterm():
    raise KeyboardInterrupt()


def read_hlpa_spec_from_env(fn_name: str, lookback: int, deployment_pattern: str) -> HorizontalLatencyPodOptimizerSpec:
    pattern = 'schedule_scale_opt_hlpa'
    min_replicas = int(os.environ.get(f'schedule_scale_opt_min_replicas_{fn_name}', 1))
    max_replicas = os.environ.get(f'schedule_scale_opt_max_replicas_{fn_name}', None)
    if max_replicas is not None:
        max_replicas = int(max_replicas)
    threshold_tolerance = float(os.environ.get(f'{pattern}_threshold_tolerance_{fn_name}', 0.1))
    target_duration = os.environ.get(f'{pattern}_duration_target_{fn_name}', 100)
    target_duration = float(target_duration)
    percentile_duration = os.environ.get(f'{pattern}_duration_percentile_{fn_name}', 90)
    percentile_duration = int(percentile_duration)
    time_measure = os.environ.get('schedule_scale_opt_hlpa_time_measure_target')
    return HorizontalLatencyPodOptimizerSpec(
        target_deployment=fn_name,
        min_replicas=min_replicas,
        max_replicas=max_replicas,
        lookback=lookback,
        deployment_pattern=deployment_pattern,
        threshold_tolerance=threshold_tolerance,
        target_duration=target_duration,
        percentile_duration=percentile_duration,
        target_time_measure=time_measure
    )


def read_all_hlpa_parameters_from_env(lookback: int, deployment_pattern: str) -> Dict[
    str, HorizontalLatencyPodOptimizerSpec]:
    specs = {}
    for key in os.environ.keys():
        pattern = 'schedule_scale_opt_hlpa_duration_target_'
        if pattern in key:
            fn = key.replace(pattern, '')
            spec = read_hlpa_spec_from_env(fn, lookback, deployment_pattern)
            specs[fn] = spec
    return specs


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
        time.sleep(5)
        ctx = ctx_daemon.context
        lookback = int(os.environ.get('schedule_scale_opt_lookback', 60))

        optimizer = HorizontalLatencyPodOptimizer(read_all_hlpa_parameters_from_env(lookback, deployment_pattern))
        k8s_client = KubernetesClient.from_env()
        consumers = [RedisHlpaResultConsumer(rds_client)]
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
