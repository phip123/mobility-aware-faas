import datetime
from typing import Optional, Dict

import pandas as pd

from galileocontext.telemetry import util
from galileocontext.telemetry.api import TelemetryService
from galileocontext.telemetry.rds import Resources


class StaticTelemetryService(TelemetryService):

    def __init__(self, container_resources: Dict[str, Resources], node_resources: Dict[str, Resources]):
        self.container_resources = container_resources
        self.node_resources = node_resources

    def get_container_cpu(self, container_id: str, start: datetime.datetime = None, end: datetime.datetime = None) -> \
            Optional[pd.DataFrame]:
        return util.get_container_cpu(self.container_resources, container_id, start, end)

    def get_node_cpu(self, node: str) -> Optional[pd.DataFrame]:
        return util.get_node_cpu(self.node_resources, node)
