from typing import List

from galileocontext.deployments.api import DeploymentService, Deployment


class StaticDeploymentService(DeploymentService):

    def __init__(self, deployments: List[Deployment]):
        self.deployments = deployments

    def get_deployments(self) -> List[Deployment]:
        return self.deployments
