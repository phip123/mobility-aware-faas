import logging
import os
import threading
import uuid
from typing import Dict, Union

from kubernetes.client import V1ResourceRequirements, V1NodeSelector, V1NodeSelectorTerm, V1NodeSelectorRequirement

from galileocontext.constants import function_label, worker_role_label, zone_label
from schedulescaleopt.opt.api import PodRequest

logger = logging.getLogger(__name__)

from kubernetes import client


def create_pod_with_request(v1: client.CoreV1Api, request: PodRequest):
    return create_pod(v1, image=request.image, labels=request.labels, pod_name=request.pod_name.replace('_', '-'),
                      container_name=request.labels[function_label].replace('_', '-'),
                      resource_requests=request.resource_requests, namespace=request.namespace)


def create_pod_with_image(v1: client.CoreV1Api, image: str, labels: Dict[str, str], resource_requests: Dict[str, str],
                          namespace: str = "default"):
    sanitized_image = image.replace(":", "-").replace(".", "-")
    slash_index = sanitized_image.rindex("/") + 1
    sanitized_image = sanitized_image[slash_index:]
    uuid_ = str(uuid.uuid4())[:6]
    pod_name = f"{sanitized_image}-{uuid_}"
    container_name = sanitized_image
    return create_pod(v1, namespace, pod_name, container_name, image, labels, resource_requests=resource_requests)


def create_pod(v1: client.CoreV1Api, namespace: str, pod_name: str, container_name: str, image: str,
               labels: Dict[str, str], resource_requests: Dict[str, str]) -> Union[client.V1Pod, threading.Thread]:
    """
    Creates a Pod

    References:
    * V1Pod: https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Pod.md
    * * V1ObjectMeta: https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1ObjectMeta.md
    * * V1PodSpec: https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1PodSpec.md
    * * * V1Container: https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Container.md
    * * * V1ContainerPort: https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1ContainerPort.md
    * * * V1ResourceRequirements: https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1ResourceRequirements.md
    :param v1:
    :param namespace:
    :param pod_name:
    :param container_name:
    :param image:
    :param labels:
    :return:
    """
    if labels.get('pod-template-hash', None) is not None:
        del labels['pod-template-hash']
    selector = {worker_role_label: 'true'}

    if labels.get(zone_label, None) is not None:
        selector[zone_label] = labels[zone_label]

    pod = client.V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=client.V1ObjectMeta(name=pod_name, labels=labels),
        spec=client.V1PodSpec(
            node_selector=selector,
            containers=[
                client.V1Container(
                    image=image,
                    name=container_name,
                    ports=[
                        client.V1ContainerPort(
                            name="function-port", container_port=8080
                        )
                    ],
                    resources=V1ResourceRequirements(
                        requests=resource_requests
                    )
                )
            ]
        ),
    )
    # logger.info(f"Creater pod '{pod_name}'")
    async_req = os.getenv("schedule_scale_opt_async_pod_creation", "False")
    async_req = async_req == "True"
    return v1.create_namespaced_pod(namespace, pod, async_req=async_req)
