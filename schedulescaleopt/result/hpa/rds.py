import logging
from typing import List

from galileocontext.connections import RedisClient
from schedulescaleopt.opt.api import ScaleScheduleEvent, HpaScalerSchedulerResult, HpaDecision
from schedulescaleopt.result.api import ResultConsumer

logger = logging.getLogger(__name__)


class RedisHpaResultConsumer(ResultConsumer):

    def __init__(self, rds: RedisClient, channel: str = 'galileo/events'):
        self.rds = rds
        self.channel = channel

    def _publish(self, msgs: List, name, clz):
        for msg in msgs:
            logger.debug(msg)
            ts = msg.ts
            rds_msg = f'{ts} {name} {clz.to_json(msg)}'
            self.rds.publish_async(self.channel, rds_msg)

    def consume(self, scaler: HpaScalerSchedulerResult):
        logger.info("redis result consumer is publishing the results...")

        self._publish(scaler.decisions, 'decision', HpaDecision)
        self._publish(scaler.scale_schedule_events, 'scale_schedule', ScaleScheduleEvent)
