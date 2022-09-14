import abc
import json
from dataclasses import dataclass
from typing import Dict, List, Optional

from galileodb import NodeInfo

from galileocontext.constants import zone_label


@dataclass
class Node:
    name: str
    arch: str
    cpus: int
    ram: int
    boot: int
    disk: List[str]
    net: List[str]
    hostname: str
    netspeed: str
    labels: Dict[str, str]
    zone: Optional[str]
    allocatable: Dict[str, str]
    cluster: str = ""


def map_node_info_to_node(nodeinfo: NodeInfo) -> Node:
    name = nodeinfo.node
    arch = nodeinfo.data['arch']
    cpus = int(nodeinfo.data['cpus'])
    ram = int(nodeinfo.data['ram'])
    boot = int(nodeinfo.data['boot'])
    disk = nodeinfo.data['disk'].split(' ')
    net = nodeinfo.data['net'].split(' ')
    netspeed = nodeinfo.data['netspeed']
    labels = json.loads(nodeinfo.data['labels'])
    zone = labels.get(zone_label, None)
    allocatable = json.loads(nodeinfo.data['allocatable'])
    cpu = allocatable.get('cpu', None)
    if cpu is not None:
        if cpu == '1':
            allocatable['cpu'] = '1000m'

    return Node(
        name,
        arch,
        cpus,
        ram,
        boot,
        disk,
        net,
        name,
        netspeed,
        labels,
        zone,
        allocatable
    )


class NodeService(abc.ABC):

    def get_zones(self) -> List[str]:
        raise NotImplementedError()

    def get_nodes(self) -> List[Node]:
        raise NotImplementedError()

    def find(self, nodeName: str) -> Optional[Node]:
        raise NotImplementedError()

    def find_nodes_in_zone(self, zone: str) -> List[Node]:
        raise NotImplementedError()
