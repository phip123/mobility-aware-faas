import json
import os
from typing import Tuple, Dict

from galileocontext.network.api import NetworkService
from galileocontext.util.rwlock import ReadWriteLock


class StaticNetworkService(NetworkService):

    def __init__(self, min_latency: float, max_latency: float, latency_map: Dict[Tuple[str, str], float]):
        self.max_latency = max_latency
        self.min_latency = min_latency
        self.latency_map = latency_map
        self.rw_lock = ReadWriteLock()

    def get_latency(self, from_node: str, to_node: str) -> float:
        latency = self.latency_map.get((from_node, to_node), None)
        if latency is None:
            latency = self.latency_map.get((to_node, from_node), None)
            if latency is None:
                raise ValueError(f'No latency for connection: {from_node} - {to_node}')
        return latency

    def get_max_latency(self) -> float:
        return self.max_latency

    def update_latency(self, from_node: str, to_node: str, value: float):
        self.rw_lock.acquire_write()
        curr_avg = self.latency_map[(from_node, to_node)]
        if curr_avg == 0:
            new_avg = value
        else:
            new_avg = (curr_avg + value) / 2
        # if new_avg > self.max_latency:
        #     self.max_latency = new_avg
        self.latency_map[(from_node, to_node)] = new_avg
        self.latency_map[(to_node, from_node)] = new_avg
        self.rw_lock.release_write()

    @classmethod
    def from_env(cls) -> 'StaticNetworkService':
        json_file = os.environ.get('galileo_context_latency_graph_json', None)
        max_latency = int(os.environ.get('galileo_context_max_latency', 0))
        min_latency = None
        if json_file is not None:
            with open(json_file, 'r') as fd:
                json_latency_map: Dict[str, Dict[str, float]] = json.load(fd)
                latency_map = {}
                for from_node, to_values in json_latency_map.items():
                    for to_node, latency in to_values.items():
                        latency_map[(from_node, to_node)] = latency
                        if latency > max_latency:
                            max_latency = latency
                        if min_latency is None or min_latency > latency:
                            min_latency = latency
                    latency_map[(from_node, from_node)] = 0
                return StaticNetworkService(min_latency, max_latency, latency_map)
        else:
            return StaticNetworkService(-1, -1, {})
