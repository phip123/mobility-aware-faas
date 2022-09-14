import logging
import time

from galileocontext.context.cluster import Context
from lbopt.opt.api import WeightOptimizer
from lbopt.publisher.api import WeightPublisher
from lbopt.weights.api import WeightManager

logger = logging.getLogger(__name__)

class OptDaemon:

    def __init__(self, ctx: Context, weight_manager: WeightManager, optimizer: WeightOptimizer, weight_publisher: WeightPublisher,
                 reconcile_interval: float):
        """
        :param reconcile_interval: time to wait before next optimization iteration, in seconds
        """
        self._stop = False
        self.reconcile_interval = reconcile_interval
        self.optimizer = optimizer
        self.weight_manager = weight_manager
        self.ctx = ctx
        self.weight_publisher = weight_publisher

    def run(self):
        logger.info("start lbopt daemon")
        while not self._stop:
            logger.info("run lbopt optimization")
            updates = self.optimizer.run(self.ctx)
            logger.debug(f"lbopt optimization result: {updates}")
            for deployment_update in updates:
                for zone_update in deployment_update:
                    self.weight_manager.update(zone_update)
                    self.weight_publisher.publish(zone_update)
            logger.info("lbop daemon updated etcd, back to sleep...")
            time.sleep(self.reconcile_interval)

    def stop(self):
        self._stop = True
