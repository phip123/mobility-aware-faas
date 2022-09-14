from typing import List

from galileocontext.deployments.api import DeploymentService, Deployment
from galileocontext.pod.api import PodService


class PodExtractedDeploymentService(DeploymentService):

    def __init__(self, pod_service: PodService, deployment_pattern: str = '-deployment'):
        self.pod_service = pod_service
        self.deployment_pattern = deployment_pattern

    def get_deployments(self) -> List[Deployment]:
        deployments = {}
        for pod in self.pod_service.get_pod_containers():
            name = pod.name
            index = name.find(self.deployment_pattern)
            if index == -1:
                continue
            function_name = name[:index]
            deployment = Deployment(
                original_name=f"{function_name}{self.deployment_pattern}", fn_name=function_name.replace('-', '_'),
                image=pod.image,
                namespace=pod.namespace, labels=pod.labels, resource_requests=pod.resource_requests, parsed_resource_requests=pod.parsed_resource_requests)
            deployments[deployment.fn_name] = deployment
        return list(deployments.values())
