import logging
import os
import threading
from typing import Optional

from telemc import TelemetryController

from galileocontext.connections import RedisClient
from galileocontext.context.cluster import Context
from galileocontext.deployments.rds import PodExtractedDeploymentService
from galileocontext.network.api import NetworkService
from galileocontext.network.static import StaticNetworkService
from galileocontext.nodes.rds import TelemcNodeService
from galileocontext.pod.rds import RedisPodService
from galileocontext.telemetry.rds import RedisTelemetryService
from galileocontext.traces.rds import RedisTracesService
from galileocontext.zones.static import StaticZoneService

logger = logging.getLogger(__name__)


class DefaultGalileoContextDaemon:

    def __init__(self, fn_pattern: str, network_service: NetworkService=None):
        """

        :param fn_pattern: tells the services which Pods and Deployments should be considered (i.e., if you
        deploy a Deployment called 'nginx-deployment' and another one called 'nginx-function', while passing '-function'
        as the pattern. Only the pods belonging to 'nginx-function' will considered as deployment
        """
        self.fn_pattern = fn_pattern
        self.t = None
        self._context = None
        self.rds = None
        self.pod_service = None
        self.deployment_service = None
        self.telemc = None
        self.node_service = None
        self.trace_service = None
        self.telemetry_service = None
        self.zone_service = None
        self.network_service = network_service

    def run(self):
        logging.basicConfig(level=logging._nameToLevel[os.environ.get('galileo_context_logging', 'DEBUG')])

        self.rds = RedisClient.from_env()
        self.zone_service = StaticZoneService(['zone-a', 'zone-b','zone-c'])
        self.telemc = TelemetryController(self.rds.conn())
        self.node_service = TelemcNodeService(self.telemc)
        self.pod_service = RedisPodService(self.rds, self.node_service)
        self.deployment_service = PodExtractedDeploymentService(self.pod_service, self.fn_pattern)
        telemetry_window_size = int(os.environ.get('galileo_context_telemetry_window_size', 60))
        traces_window_size = int(os.environ.get('galileo_context_trace_window_size', 60))
        self.telemetry_service = RedisTelemetryService(telemetry_window_size, self.rds, self.node_service)
        if self.network_service is None:
            self.network_service = StaticNetworkService.from_env()
        self.trace_service = RedisTracesService(traces_window_size, self.rds, self.pod_service, self.node_service,
                                                self.network_service)
        self._context = Context(
            self.rds,
            self.pod_service,
            self.telemetry_service,
            self.deployment_service,
            self.node_service,
            self.trace_service,
            self.telemc,
            self.zone_service,
            self.network_service
        )

        self.telemetry_service.start()
        self.trace_service.start()
        self.pod_service.start()

    @property
    def context(self) -> Optional[Context]:
        return self._context

    def start(self):
        self.t = threading.Thread(target=self.run)
        self.t.start()

    def stop(self):
        if self.t is not None:
            self.t.join()
