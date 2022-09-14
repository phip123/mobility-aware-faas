import logging
import os
import signal
import time

import redis

from galileocontext.connections import EtcdClient, RedisClient
from galileocontext.daemon import DefaultGalileoContextDaemon
from lbopt.daemon import OptDaemon
from lbopt.opt.threshold import ThresholdBasedWeightOptimizer
from lbopt.publisher.rds import RedisWeightPublisher
from lbopt.weights.etcd import EtcdWeightManager

logger = logging.getLogger(__name__)


def handle_sigterm():
    raise KeyboardInterrupt()


def main():
    signal.signal(signal.SIGTERM, handle_sigterm)

    logging.basicConfig(level=logging._nameToLevel[os.environ.get('osmotic_lb_opt_logging', 'DEBUG')])
    logger.info("Start osmotic_lb_opt")
    reconcile_interval = int(os.environ.get('osmotic_lb_opt_reconcile_interval', 10))
    ctx_daemon = None
    rds = None
    etcd_client = EtcdClient.from_env()
    try:
        ctx_daemon = DefaultGalileoContextDaemon('-deployment')
        ctx_daemon.start()
        time.sleep(2)
        ctx = ctx_daemon.context
        weight_manager = EtcdWeightManager(etcd_client)
        threshold_based_weight_optimizer = ThresholdBasedWeightOptimizer.read_from_env()
        rds = RedisClient.from_env()
        rds_weight_publisher = RedisWeightPublisher(rds)
        daemon = OptDaemon(ctx, weight_manager, threshold_based_weight_optimizer, rds_weight_publisher,
                           reconcile_interval)
        daemon.run()
    except KeyboardInterrupt:
        logger.info("Received interrupt. Shutting down...")
    finally:
        if ctx_daemon is not None:
            ctx_daemon.stop()
        if rds is not None:
            rds.close()


if __name__ == '__main__':
    main()
