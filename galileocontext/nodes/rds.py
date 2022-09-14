import logging
from typing import List, Optional, Dict

import redis
from galileodb import NodeInfo
from telemc import TelemetryController

from galileocontext.nodes.api import NodeService, Node, map_node_info_to_node

logger = logging.getLogger(__name__)

class TelemcNodeService(NodeService):
    _node_infos: List[NodeInfo]
    _nodes: Dict[str, Node]

    def __init__(self, telemc: TelemetryController):
        self._node_infos = []
        self._nodes = {}
        self.telemc = telemc

    def get_zones(self) -> List[str]:
        zones = set()
        for node in self.get_nodes():
            zone = node.zone
            if zone is not None:
                zones.add(zone)
        return list(zones)

    def get_nodes(self) -> List[Node]:
        if len(self._node_infos) == 0:
            self._node_infos = self.telemc.get_node_infos()
            logger.debug(self._node_infos)
            for node_info in self._node_infos:
                self._nodes[node_info.node] = map_node_info_to_node(node_info)
        return list(self._nodes.values())

    def find(self, node_name: str) -> Optional[Node]:
        nodes = self.get_nodes()
        for node in nodes:
            if node.name == node_name:
                return node
        return None


    def find_nodes_in_zone(self, zone: str) -> List[Node]:
        return [x for x in self.get_nodes() if x.zone == zone]


if __name__ == '__main__':
    rds = redis.Redis(decode_responses=True)
    telemc = TelemetryController(rds)
    service = TelemcNodeService(telemc)
    print(service.get_nodes())
    print(service.get_zones())
