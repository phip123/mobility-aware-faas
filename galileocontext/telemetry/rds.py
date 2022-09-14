import datetime
import logging
import threading
from typing import Dict, Optional

import pandas as pd

from galileocontext import Point
from galileocontext.connections import RedisClient
from galileocontext.nodes.api import NodeService
from galileocontext.telemetry import util
from galileocontext.telemetry.api import TelemetryService, Resources

logger = logging.getLogger(__name__)


def parse_channel(event):
    channel = event['channel']
    split_channel = channel.split('/')
    node = split_channel[1]
    metric = split_channel[2]
    return metric, node, split_channel


def parse_data(event):
    msg = event['data']
    split = msg.split(' ', maxsplit=2)
    ts = float(split[0])
    val = float(split[1])
    return msg, ts, val


class RedisTelemetryService(TelemetryService):
    # key is container id
    container_resources: Dict[str, Resources]

    # key is node name
    node_resources: Dict[str, Resources]

    def __init__(self, window_size: int, rds_client: RedisClient, node_service: NodeService):
        self.window_size = window_size
        self.node_service = node_service
        self.rds_client = rds_client
        self.t: threading.Thread = None
        self._container_metrics = ["kubernetes_cgrp_cpu", "kubernetes_cgrp_mem", "kubernetes_cgrp_blkio"]
        self._node_metrics = ["cpu", "ram", "load"]
        self.container_resources = {}
        self.node_resources = {}

    def get_container_cpu(self, container_id: str, start: datetime.datetime = None, end: datetime.datetime = None) -> \
    Optional[pd.DataFrame]:
        resources = self.container_resources.get(container_id, None)
        if resources is None:
            return None
        cores = self.node_service.find(resources.node).cpus
        return util.get_container_cpu(self.container_resources, container_id,cores, start, end)

    def get_node_cpu(self, node: str):
        return util.get_node_cpu(self.node_resources, node)

    def save_container_metric(self, node: str, metric: str, container_id: str, ts: float, val: float):
        resources = self.container_resources.get(container_id, None)
        if resources is None:
            self.container_resources[container_id] = Resources(node, self.window_size, self._container_metrics)
            resources = self.container_resources[container_id]

        window = Point(ts=ts, val=val)
        resources.append(metric, window)

    def save_node_metric(self, node: str, metric: str, ts: float, val: float):
        resources = self.node_resources.get(node, None)
        if resources is None:
            self.node_resources[node] = Resources(node, self.window_size, self._node_metrics)
            resources = self.node_resources[node]

        window = Point(ts=ts, val=val)
        resources.append(metric, window)

    def run(self):
        for event in self.rds_client.psub('telem/*'):
            # logger.debug("got event", event)
            # channel parsing
            metric, node, split_channel = parse_channel(event)

            # data parsing
            msg, ts, val = parse_data(event)

            if len(split_channel) == 4 and metric != 'rd' and metric != 'wr' and metric != 'rx' and metric != 'tx':
                # resource with subsystem
                # TODO differentiate between container as subsystem and host related metrics (i.e., rd) and subsystem
                container_id = split_channel[3]
                self.save_container_metric(node, metric, container_id, ts, val)

            elif len(split_channel) == 3:
                # node resource
                self.save_node_metric(node, metric, ts, val)

    def start(self):
        self.t = threading.Thread(target=self.run)
        self.t.start()
        return self.t

    def stop(self):
        if self.t is not None:
            self.t.join(5)


if __name__ == '__main__':
    rds_client = RedisClient.from_env()
    service = RedisTelemetryService(2, rds_client)
    service.run()
