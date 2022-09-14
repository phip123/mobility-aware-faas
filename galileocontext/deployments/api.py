import abc
from dataclasses import dataclass
from typing import Dict, List


@dataclass
class Deployment:
    original_name: str
    fn_name: str
    image: str
    namespace: str
    labels: Dict[str, str]
    resource_requests: Dict[str,str]
    parsed_resource_requests: Dict[str,float]


class DeploymentService(abc.ABC):

    def get_deployments(self) -> List[Deployment]:
        raise NotImplementedError()
