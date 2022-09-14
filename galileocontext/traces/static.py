import pandas as pd

from galileocontext.constants import zone_label
from galileocontext.nodes.api import NodeService
from galileocontext.traces.api import TracesService


class StaticTracesService(TracesService):

    def __init__(self, traces: pd.DataFrame, node_service: NodeService):
        self.node_service = node_service
        self.traces = traces

    def get_traces_api_gateway(self, hostname: str) -> pd.DataFrame:
        return self.traces[
            self.traces['dest_zone'] == self.node_service.find(hostname).labels.get(zone_label, 'none')]
