import abc
import logging
from copy import copy
from dataclasses import dataclass, field
from typing import List, Optional, Dict

from dataclasses_json import dataclass_json

from galileocontext.constants import function_label
from galileocontext.nodes.api import Node
from schedulescaleopt.util.storage import parse_size_string_to_bytes

logger = logging.getLogger(__name__)


def parse_cpu_millis(cpu: str) -> int:
    cpu = int(cpu.replace('m', '').replace('"', ''))
    if cpu <= 10:
        # in case resource request is 1 (== 1 core) == 1000m
        return cpu * 1000
    return cpu


# Defaults taken from:
# https://github.com/kubernetes/kubernetes/blob/4c659c5342797c9a1f2859f42b2077859c4ba621/pkg/scheduler/util/pod_resources.go#L25
default_milli_cpu_request = '100m'  # 0.1 core
default_mem_request = f'{200}Mi'  # 200 MB


@dataclass_json
@dataclass
class Container:
    id: str
    name: str
    image: str
    port: int
    resource_requests: Dict[str, str]
    resource_limits: Dict[str, str]


@dataclass_json
@dataclass
class Pod:
    podUid: str
    name: str
    namespace: str
    qosClass: str
    labels: Dict[str, str]
    status: str
    startTime: str = ""
    podIP: str = ""
    nodeName: str = ""
    hostIP: str = ""
    containers: Dict[str, Container] = field(default_factory=dict)


@dataclass
class PodContainer:
    podUid: str
    name: str
    hostIP: str
    nodeName: str
    podIP: str
    qosClass: str
    startTime: str
    labels: Dict[str, str]
    container_id: str
    image: str
    port: int
    url: str
    namespace: str
    status: str
    resource_requests: Dict[str, str] = field(default_factory=dict)
    parsed_resource_requests: Dict[str, float] = field(default_factory=dict)
    resource_limits: Dict[str, str] = field(default_factory=dict)


def parse_pod_container(text: str) -> Optional[PodContainer]:
    try:
        pod = Pod.from_json(text)
        ip = ""
        container_id = ""
        container_image = ""
        port = ""
        url = ""
        container_requests = {}
        parsed_request = {}
        resource_limits = {}
        if len(pod.containers) > 0:
            container = list(pod.containers.values())[0]
            container_requests = container.resource_requests
            container_id = container.id.replace('containerd://', '')
            container_image = container.image
            resource_limits = container.resource_limits

            port = container.port
            ip = pod.podIP
            url = f'{ip}:{port}'

            for k, v in container_requests.items():
                container_requests[k] = v.replace('"', '')

            memory_request = container_requests.get('memory', default_mem_request)
            parsed_memory_request = parse_size_string_to_bytes(memory_request)
            cpu_request = container_requests.get('cpu', default_milli_cpu_request)
            parsed_cpu_request = parse_cpu_millis(cpu_request)

            container_requests['cpu'] = cpu_request
            container_requests['memory'] = memory_request

            parsed_request = {
                'cpu': parsed_cpu_request,
                'memory': parsed_memory_request
            }

        labels = copy(pod.labels)
        fn_label_value = labels.get(function_label, None)
        if fn_label_value is not None:
            labels[function_label] = fn_label_value.replace('-', '_')

        return PodContainer(
            pod.podUid,
            pod.name,
            pod.hostIP,
            pod.nodeName,
            ip,
            pod.qosClass,
            pod.startTime,
            labels,
            container_id,
            container_image,
            port,
            url,
            pod.namespace,
            pod.status,
            container_requests,
            parsed_request,
            resource_limits
        )
    except Exception as e:
        logger.error(e)
        return None


class PodService(abc.ABC):

    def get_pod_containers(self) -> List[PodContainer]:
        raise NotImplementedError()

    def get_pod_container_with_ip(self, pod_ip: str, running: bool = True, status: str = None) -> Optional[
        PodContainer]:
        raise NotImplementedError()

    def get_pod_containers_of_deployment(self, name, running: bool = True, status: str = None) -> List[PodContainer]:
        """
        param name: original name of the deployment
        """
        raise NotImplementedError()

    def find_pod_containers_with_labels(self, labels: Dict[str, str] = None, node_labels=None,
                                        running: bool = True, status: str = None) -> List[
        PodContainer]:
        raise NotImplementedError()

    def get_pod_container_by_name(self, name: str) -> Optional[PodContainer]:
        raise NotImplementedError()

    def get_pod_container_with_id(self, container_id: str) -> Optional[PodContainer]:
        raise NotImplementedError()

    def get_pod_containers_on_node(self, node: Node) -> List[PodContainer]:
        raise NotImplementedError()

    def shutdown_pod(self, pod_uid: str):
        raise NotImplementedError()

    def add_pod(self, pod: PodContainer):
        raise NotImplementedError()
