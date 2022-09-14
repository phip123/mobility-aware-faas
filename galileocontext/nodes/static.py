from typing import List, Optional

from galileocontext.nodes.api import NodeService, Node


class StaticNodeService(NodeService):

    def __init__(self, zones: List[str], nodes: List[Node]):
        self.zones = zones
        self.nodes = nodes

    def get_zones(self) -> List[str]:
        return self.zones

    def get_nodes(self) -> List[Node]:
        return self.nodes

    def find(self, nodeName: str) -> Optional[Node]:
        for node in self.nodes:
            if node.name == nodeName:
                return node
        return None

    def find_nodes_in_zone(self, zone: str) -> List[Node]:
        collected = []
        for node in self.nodes:
            if node.zone == zone:
                collected.append(node)

        return collected