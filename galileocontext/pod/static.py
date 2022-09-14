from typing import Dict, List, Optional

from galileocontext.nodes.api import Node
from galileocontext.pod.api import PodService, PodContainer


class StaticPodService(PodService):

    def get_pod_containers_on_node(self, node: Node) -> List[PodContainer]:
        return list(filter(lambda p: p.node_name == node.name, self.pod_containers))

    def get_pod_container_by_name(self, name: str) -> Optional[PodContainer]:
        pods = list(filter(lambda p: p.name == name, self.pod_containers))
        return pods[0] if len(pods) == 1 else None

    def __init__(self, pod_containers: List[PodContainer]):
        self.pod_containers = pod_containers

    def get_pod_containers(self) -> List[PodContainer]:
        return self.pod_containers

    def get_pod_container_with_ip(self, pod_ip: str) -> Optional[PodContainer]:
        for pod in self.pod_containers:
            if pod.podIP == pod_ip:
                return pod
        return None

    def get_pod_containers_of_deployment(self, name) -> List[PodContainer]:
        collected = []
        for pod in self.pod_containers:
            if name in pod.name:
                collected.append(pod)
        return collected

    def find_pod_containers_with_labels(self, labels: Dict[str, str] = None, node_labels: Dict[str, str] = None) -> \
    List[PodContainer]:
        pods = []
        for pod in self.pod_containers:
            matches = True
            if labels is not None:
                pod_labels = pod.labels
                for label in labels:
                    pod_label_value = pod_labels.get(label, None)
                    if pod_label_value is None or pod_label_value != labels[label]:
                        matches = False
                        break
            # TODO implement node_labels check
            if matches:
                pods.append(pod)
        return pods

    def get_pod_container_with_id(self, container_id: str) -> Optional[PodContainer]:
        for pod in self.pod_containers:
            if pod.container_id == container_id:
                return pod

        return None
