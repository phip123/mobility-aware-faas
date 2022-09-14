import logging
import time
from typing import List

from galileocontext.context.cluster import Context
from schedulescaleopt.opt.api import ScalerSchedulerOptimizer
from schedulescaleopt.result.api import ResultConsumer

logger = logging.getLogger(__name__)


class OptDaemon:

    def __init__(self, ctx: Context, optimizer: ScalerSchedulerOptimizer, consumers: List[ResultConsumer], reconcile_interval: float):
        """
        :param reconcile_interval: time to wait before next optimization iteration, in seconds
        """
        self._stop = False
        self.reconcile_interval = reconcile_interval
        self.optimizer = optimizer
        self.ctx = ctx
        self.consumers = consumers

    def run(self):
        logger.info("start osmotic opt daemon")
        while not self._stop:
            logger.info("run optimization")
            result = self.optimizer.run(self.ctx)
            logger.info(f"optimization result: {result}")

            for consumer in self.consumers:
                consumer.consume(result)


            logger.info("scaling scheduling published, back to sleep...")
            time.sleep(self.reconcile_interval)

    def stop(self):
        self._stop = True
