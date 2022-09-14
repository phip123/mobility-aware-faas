import logging

from galileocontext.connections import EtcdClient
from lbopt.weights.api import WeightManager, WeightUpdate, FunctionState, Weights

logger = logging.getLogger(__name__)


class EtcdWeightManager(WeightManager):

    def __init__(self, client: EtcdClient):
        self.client = client

    def read_state(self) -> FunctionState:
        logging.info('Read function state from etcd')
        return FunctionState({})

    def update(self, weight_update: WeightUpdate):
        logging.info(f'Got weight update: {weight_update}')
        key = f'golb/function/{weight_update.zone}/{weight_update.fn}'
        value = Weights.to_json(weight_update.weights)
        self.client.write(key=key, value=value)