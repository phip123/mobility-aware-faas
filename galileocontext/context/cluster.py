import time
from dataclasses import dataclass
from typing import List

from telemc import TelemetryController

from galileocontext.connections import RedisClient
from galileocontext.deployments.api import DeploymentService
from galileocontext.network.api import NetworkService
from galileocontext.nodes.api import NodeService
from galileocontext.pod.api import PodContainer, PodService
from galileocontext.telemetry.api import TelemetryService
from galileocontext.traces.api import TracesService
from galileocontext.zones.api import ZoneService


@dataclass
class Context:
    rds: RedisClient
    pod_service: PodService
    telemetry_service: TelemetryService
    deployment_service: DeploymentService
    node_service: NodeService
    trace_service: TracesService
    telemc: TelemetryController
    zone_service: ZoneService
    network_service: NetworkService


class ClusterContext:
    pod_containers: List[PodContainer]

    def __init__(self, ctx: Context):
        self.ctx = ctx
        self._last_refresh = time.time()
        self.pod_service = ctx.pod_service
        self.telemetry_service = ctx.telemetry_service
        self.node_service = ctx.node_service
        self.deployment_service = ctx.deployment_service
        self.trace_service = ctx.trace_service
        self.zone_service = ctx.zone_service
        self.network_service = ctx.network_service

    def _get_containers(self) -> List[PodContainer]:
        return self.pod_service.get_pod_containers()

    def get_containers(self) -> List[PodContainer]:
        return self.pod_service.get_pod_containers()
