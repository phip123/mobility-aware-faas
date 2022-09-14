import redis

from galileocontext.connections import RedisClient
from lbopt.publisher.api import WeightPublisher
from lbopt.weights.api import WeightUpdate


class RedisWeightPublisher(WeightPublisher):

    def __init__(self, rds: RedisClient, channel: str = 'galileo/events'):
        self.rds = rds
        self.channel = channel

    def publish(self, update: WeightUpdate):
        ts = update.ts
        rds_msg = f'{ts} weight_update {WeightUpdate.to_json(update)}'
        self.rds.publish_async(self.channel, rds_msg)