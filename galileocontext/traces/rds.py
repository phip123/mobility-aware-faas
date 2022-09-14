import datetime
import json
import logging
import re
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Optional

import pandas as pd
from galileodb.model import RequestTrace
from galileodb.recorder.traces import TracesSubscriber

from galileocontext import PointWindow, Point
from galileocontext.connections import RedisClient
from galileocontext.constants import function_label, client_role_label
from galileocontext.deployments.api import Deployment
from galileocontext.network.api import NetworkService
from galileocontext.nodes.api import NodeService, Node
from galileocontext.pod.api import PodService, PodContainer
from galileocontext.traces.api import TracesService
from galileocontext.util.network import update_latencies
from galileocontext.util.rwlock import ReadWriteLock

logger = logging.getLogger(__name__)


@dataclass
class PodRequestTrace:
    # pod that executed the request
    pod_container: PodContainer

    # node on which the request was processed
    node: Node

    trace: RequestTrace

    @property
    def origin_zone(self) -> Optional[str]:
        galileo_worker_zone_pattern = "zone-.{1}"
        try:
            text = self.trace.client
            return re.search(galileo_worker_zone_pattern, text).group(0)
        except (AttributeError, IndexError):
            logger.warning(f"can't find zone pattern in client {self.trace.client}")
            return None

    @property
    def destination_zone(self) -> Optional[str]:
        return self.node.zone


class RedisTracesService(TracesService):



    def __init__(self, window_size: int, rds_client: RedisClient, pod_service: PodService, node_service: NodeService,
                 network_service: NetworkService):
        self.window_size = window_size
        self.rds_client = rds_client
        self.traces_subscriber = TracesSubscriber(rds_client.conn())
        self.pod_service = pod_service
        self.node_service = node_service
        self.network_service = network_service
        self.locks: Dict[str, ReadWriteLock] = self.init_locks(node_service)
        self.requests_per_node: Dict[str, PointWindow[PodRequestTrace]] = {}

    def init_locks(self, node_service: NodeService) -> Dict[str, ReadWriteLock]:
        locks = {}
        for node in node_service.get_nodes():
            locks[node.name] = ReadWriteLock()
        return locks

    def get_traces_for_fn(self, fn: str, start, end, zone: str = None):
        if zone is not None:
            nodes = self.node_service.find_nodes_in_zone(zone)
        else:
            nodes = self.node_service.get_nodes()
        requests = defaultdict(list)
        for node in nodes:
            node_name = node.name
            self.locks[node_name].acquire_read()
            node_requests = self.requests_per_node.get(node_name)
            if node_requests is None or node_requests.size() == 0:
                self.locks[node_name].release_read()
                continue

            for req in node_requests.value():
                if req.val.pod_container.labels[function_label] == fn:
                    self.parse_request(node_name, req, requests)

            self.locks[node_name].release_read()

        df = pd.DataFrame(data=requests)
        if len(df) == 0:
            return df
        df = df.sort_values(by='ts')
        df.index = pd.DatetimeIndex(pd.to_datetime(df['ts'], unit='s'))

        now = time.time()
        logger.info(f'Before filtering {len(df)} traces for function {fn}')
        df = df[df['ts'] >= now - self.window_size]
        logger.info(f'After filtering {len(df)} traces left for function {fn}')
        df = df[df['status'] == 200]
        logger.info(f'AFter filtering out non-200 status: {len(df)}')
        return df

    def get_traces_for_deployment(self, deployment: Deployment, start, end, zone: str = None):
        return self.get_traces_for_fn(deployment.fn_name, start, end, zone)

    def parse_request(self, node_name, req, requests):
        sent = req.val.trace.sent
        done = req.val.trace.done
        rtt = done - sent
        container = req.val.pod_container
        headers = json.loads(req.val.trace.headers)
        start_ = headers.get('X-Start', None)
        if start_ is None:
            return
        start = float(start_)
        end = float(headers['X-End'])
        requests['network_latency'].append(((start - sent) + (done - end)) * 1000)
        requests['ts'].append(done)
        requests['function'].append(container.labels.get(function_label, '-'))
        requests['image'].append(container.image)
        requests['container_id'].append(container.container_id)
        requests['node'].append(node_name)
        requests['rtt'].append(rtt)
        requests['done'].append(done)
        requests['sent'].append(sent)
        requests['origin_zone'].append(req.val.origin_zone)
        requests['dest_zone'].append(req.val.destination_zone)
        requests['client'].append(req.val.trace.client)
        requests['status'].append(req.val.trace.status)

    def get_traces_api_gateway(self, hostname: str) -> pd.DataFrame:
        gateway = self.node_service.find(hostname)
        if gateway is None:
            nodes = [x.node for x in self.node_service._nodes.keys()]
            raise ValueError(f"Node {hostname} not found, currently stored: {nodes}")
        zone = gateway.zone
        nodes = self.node_service.find_nodes_in_zone(zone)
        requests = {
            'ts': [],
            'function': [],
            'image': [],
            'container_id': [],
            'node': [],
            'rtt': [],
            'network_latency': [],
            'sent': [],
            'done': [],
            'origin_zone': [],
            'dest_zone': [],
            'client': [],
            'status': []
        }
        if len(nodes) == 0:
            logger.info(f'No nodes found in zone {zone}')
        for node in nodes:
            # TODO fix windowing, removing old traces further down by filtering DataFrame should be temporary solution
            self.locks[node.name].acquire_read()
            node_requests = self.requests_per_node.get(node.name)
            if node_requests is None or node_requests.size() == 0:
                self.locks[node.name].release_read()
                continue

            for req in node_requests.value():
                self.parse_request(node.name, req, requests)
            self.locks[node.name].release_read()
        df = pd.DataFrame(data=requests).sort_values(by='ts')
        df.index = pd.DatetimeIndex(pd.to_datetime(df['ts'], unit='s'))

        now = time.time()
        df = df[df['ts'] >= now - self.window_size]
        logger.info(f'After filtering {len(df)} traces left for api gateway {hostname}')
        df = df[df['status'] == 200]
        logger.info(f'After filtering out non-200 status: {len(df)}')
        return df

    def find_node_for_client(self, client: str) -> Optional[Node]:
        """
        :param pods: contains 'name', and 'nodeName'
        """
        for pod in self.pod_service.find_pod_containers_with_labels(node_labels={client_role_label: "true"}):
            if pod.name in client:
                node = self.node_service.find(pod.nodeName)
                return node
        return None

    def run(self):
        for trace in self.traces_subscriber.run():
            # logger.debug("Got trace %s", trace)
            if trace.status == -1:
                logger.info("Failed trace received!")
                continue
            headers = json.loads(trace.headers)
            final_host = headers.get('X-Final-Host', '').split(',')[-1].split(':')[0].replace(' ', '')
            pod = self.pod_service.get_pod_container_with_ip(final_host, running=False)
            if pod is None:
                logger.warning(f"Looked up non-existent pod ip {final_host}")
                continue
            node = self.node_service.find(pod.nodeName)
            client_node = self.find_node_for_client(trace.client)
            self.update_latencies(client_node.name, trace.sent, headers)
            if node is None:
                logger.error("Node was None when looking for the serving pod %s", pod.nodeName)
                logger.debug("all nodes stored currently %s", str([x.name for x in self.node_service.get_nodes()]))
                continue
            pod_request_trace = PodRequestTrace(pod, node, trace)
            self.locks[node.name].acquire_write()
            window = self.requests_per_node.get(node.name, None)
            if window is None:
                self.requests_per_node[node.name] = PointWindow(self.window_size)
            self.requests_per_node[node.name].append(Point(trace.sent, pod_request_trace))
            self.locks[node.name].release_write()

    def start(self):
        self.t = threading.Thread(target=self.run)
        self.t.start()
        return self.t

    def stop(self):
        self.traces_subscriber.close()

    def update_latencies(self, client_node: str, sent: float, headers: Dict[str, str]):
        """
        Updates the latencies based on the data in headers.
        First reads X-Forwarded-For to get all nodes in the request trace, and then updates the latency between each node.
        Starting with the client-gateway connection.
        """

        updates = update_latencies(client_node, sent, headers, self.pod_service)
        for nodes, latency in updates.items():
            self.network_service.update_latency(nodes[0], nodes[1], latency)
