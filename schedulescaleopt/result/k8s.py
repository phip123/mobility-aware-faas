import logging
import threading
from typing import Union

from schedulescaleopt.k8s.create import create_pod_with_request
from schedulescaleopt.k8s.delete import delete_pod
from schedulescaleopt.opt.api import ScalerSchedulerResult
from schedulescaleopt.result.api import ResultConsumer

logger = logging.getLogger(__name__)
from kubernetes import client


class KubernetesResultConsumer(ResultConsumer):

    def __init__(self, v1_core: client.CoreV1Api, deployment_pattern: str):
        self.v1_core = v1_core
        self.deployment_pattern = deployment_pattern

    def consume(self, result: ScalerSchedulerResult):
        logger.info("kubernetes result consumer is reading the result...")
        for event in result.scale_schedule_events:
            logger.info(f"consuming: {event}")
            if not event.delete:
                creation_result: Union[client.V1Pod, threading.Thread] = create_pod_with_request(self.v1_core,
                                                                                                 event.pod)
                if isinstance(creation_result, client.V1Pod):
                    pod: client.V1Pod = creation_result
                    logger.info(f"created pod synchronously: {pod.metadata}")
                else:
                    logger.info(f"created pod asynchronously")
            else:
                delete_pod(self.v1_core, event.pod.pod_name, event.pod.namespace)
                logger.info(f"deleted pod {event.pod.pod_name}")
