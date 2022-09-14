import time

import pandas as pd

from galileocontext import Point
from galileocontext.constants import zone_label, pod_type_label, api_gateway_type_label
from galileocontext.context.cluster import Context
from galileocontext.deployments.api import Deployment
from galileocontext.deployments.static import StaticDeploymentService
from galileocontext.network.api import NetworkService
from galileocontext.network.static import StaticNetworkService
from galileocontext.nodes.api import Node, NodeService
from galileocontext.nodes.static import StaticNodeService
from galileocontext.pod.api import PodContainer
from galileocontext.pod.static import StaticPodService
from galileocontext.telemetry.rds import Resources
from galileocontext.telemetry.static import StaticTelemetryService
from galileocontext.traces.static import StaticTracesService
from galileocontext.zones.static import StaticZoneService


class MockContext:
    def create(self) -> Context:
        deployments = [Deployment('resnet-deployment', 'resnet', {})]
        deployment_service = StaticDeploymentService(deployments)

        pods = [
            PodContainer(podUid='asdf23', name='resnet-deployment-12312', hostIP='10.0.1.1', nodeName='node-1',
                         podIP='34.3.21.1', qosClass='besteffort', startTime='', labels={zone_label: 'zone-a'},
                         container_id='container-1',
                         image='resnet-image-1'),
            PodContainer(podUid='asdf24', name='go-lb-deployment-12312', hostIP='10.0.1.2', nodeName='node-2',
                         podIP='34.3.21.2', qosClass='besteffort', startTime='', labels={
                    zone_label: 'zone-a', pod_type_label: api_gateway_type_label
                }, container_id='container-2',
                         image='go-lb-image-1'),
            PodContainer(podUid='asdf25', name='resnet-deployment-12444', hostIP='10.0.2.1', nodeName='node-3',
                         podIP='34.3.22.1', qosClass='besteffort', startTime='', labels={zone_label: 'zone-b'},
                         container_id='container-3',
                         image='resnet-image-1'),
            PodContainer(podUid='asdf25', name='go-lb-deployment-12333', hostIP='10.0.2.2', nodeName='node-4',
                         podIP='34.3.22.2', qosClass='besteffort', startTime='', labels={
                    zone_label: 'zone-b', pod_type_label: api_gateway_type_label
                }, container_id='container-4',
                         image='go-lb-image-1'),
            PodContainer(podUid='asdf26', name='resnet-deployment-12454', hostIP='10.0.2.3', nodeName='node-5',
                         podIP='34.3.22.3', qosClass='besteffort', startTime='', labels={zone_label: 'zone-b'},
                         container_id='container-5',
                         image='resnet-image-1'),
        ]

        pod_service = StaticPodService(pods)
        zones = ['zone-a', 'zone-b']
        zone_service = StaticZoneService(zones)
        nodes = [
            Node('node-1', 'x86', 4, 3, 2, [], [], 'node-1', '1000', {zone_label: 'zone-a'}, 'zone-a'),
            Node('node-2', 'x86', 4, 3, 2, [], [], 'node-2', '1000', {zone_label: 'zone-a'}, 'zone-a'),
            Node('node-3', 'x86', 4, 3, 2, [], [], 'node-3', '1000', {zone_label: 'zone-b'}, 'zone-b'),
            Node('node-4', 'x86', 4, 3, 2, [], [], 'node-4', '1000', {zone_label: 'zone-b'}, 'zone-b'),
            Node('node-5', 'x86', 4, 3, 2, [], [], 'node-5', '1000', {zone_label: 'zone-b'}, 'zone-b'),
        ]

        node_service = StaticNodeService(zones, nodes)

        traces_service = self.create_mock_traces_service(node_service)
        telemetry_service = self.create_mock_telemetry_service()

        network_service = self.create_mock_network_service()

        return Context(
            None,
            pod_service,
            telemetry_service,
            deployment_service,
            node_service,
            traces_service,
            None,
            zone_service,
            network_service
        )

    def create_mock_traces_service(self, node_service: NodeService) -> StaticTracesService:
        times = [
            time.time(),
            time.time() + 1,
            time.time() + 2,
            time.time() + 2,
            time.time() + 2,
            time.time() + 2,
        ]
        fn = [
            'resnet',
            'resnet',
            'resnet',
            'resnet',
            'resnet',
            'resnet',
        ]
        requests = {
            'ts': times,
            'function': fn,
            'container': [
                'resnet-image-1',
                'resnet-image-1',
                'resnet-image-1',
                'resnet-image-1',
                'resnet-image-1',
                'resnet-image-1'
            ],
            'container_id': [
                'container-1',
                'container-1',
                'container-1',
                'container-5',
                'container-5',
                'container-3',
            ],
            'node': [
                'node-1',
                'node-1',
                'node-1',
                'node-5',
                'node-5',
                'node-3'
            ],
            'rtt': [
                2,
                2,
                2,
                2,
                2,
                2,
            ],
            'sent': times,
            'done': [
                times[0] + 2,
                times[1] + 2,
                times[2] + 2,
                times[3] + 2,
                times[3] + 2,
                times[3] + 2,
            ],
            'origin_zone': [
                'zone-a',
                'zone-a',
                'zone-b',
                'zone-b',
                'zone-b',
                'zone-b',
            ],
            'dest_zone': [
                'zone-a',
                'zone-a',
                'zone-a',
                'zone-b',
                'zone-b',
                'zone-b',
            ],
            'client': [
                'client-a',
                'client-a',
                'client-b',
                'client-b',
                'client-b',
                'client-b',
            ]
        }
        traces_df = pd.DataFrame(data=requests)
        traces_df.index = pd.DatetimeIndex(pd.to_datetime(traces_df['ts'], unit='s'))
        return StaticTracesService(traces_df, node_service)

    def create_mock_telemetry_service(self):
        container_resources = self.create_container_resources()
        node_resources = self.create_node_resources()
        return StaticTelemetryService(container_resources, node_resources)

    def create_container_resources(self):
        container_1_resources = Resources(
            'node-1', 100, ['kubernetes_cgrp_cpu']
        )
        container_2_resources = Resources(
            'node-2', 100, ['kubernetes_cgrp_cpu']
        )
        container_3_resources = Resources(
            'node-3', 100, ['kubernetes_cgrp_cpu']
        )
        container_4_resources = Resources(
            'node-4', 100, ['kubernetes_cgrp_cpu']
        )
        container_5_resources = Resources(
            'node-5', 100, ['kubernetes_cgrp_cpu']
        )
        metric = 'kubernetes_cgrp_cpu'
        self.add_resources(metric, container_1_resources)
        self.add_resources(metric, container_2_resources)
        self.add_resources(metric, container_3_resources)
        self.add_resources(metric, container_4_resources)
        self.add_resources(metric, container_5_resources)
        return {'container-1': container_1_resources, 'container-2': container_2_resources,
                'container-3': container_3_resources, 'container-4': container_4_resources,
                'container-5': container_5_resources}

    def add_resources(self, metric: str, resources):
        resources.append(metric, Point(time.time() - 1, 0.2))
        resources.append(metric, Point(time.time(), 0.4))
        resources.append(metric, Point(time.time() + 1, 0.4))

    def create_node_resources(self):
        node_1_resources = Resources(
            'node-1', 100, ['cpu']
        )

        node_2_resources = Resources(
            'node-2', 100, ['cpu']
        )
        node_3_resources = Resources(
            'node-3', 100, ['cpu']
        )
        node_4_resources = Resources(
            'node-4', 100, ['cpu']
        )
        node_5_resources = Resources(
            'node-5', 100, ['cpu']
        )
        metric = 'cpu'
        self.add_resources(metric, node_1_resources)
        self.add_resources(metric, node_2_resources)
        self.add_resources(metric, node_3_resources)
        self.add_resources(metric, node_4_resources)
        self.add_resources(metric, node_5_resources)
        return {'node-1': node_1_resources, 'node-2': node_2_resources, 'node-3': node_3_resources,
                'node-4': node_4_resources,
                'node-5': node_5_resources}

    def create_mock_network_service(self) -> NetworkService:
        return StaticNetworkService(
            latency_map={
                ('node-1', 'node-1'): 0,
                ('node-2', 'node-2'): 0,
                ('node-3', 'node-3'): 0,
                ('node-4', 'node-4'): 0,
                ('node-5', 'node-5'): 0,
                ('node-1', 'node-2'): 10,
                ('node-1', 'node-3'): 100,
                ('node-1', 'node-4'): 120,
                ('node-1', 'node-5'): 125,
                ('node-2', 'node-2'): 95,
                ('node-2', 'node-4'): 160,
                ('node-2', 'node-5'): 100,
                ('node-3', 'node-4'): 5,
                ('node-3', 'node-5'): 10,
                ('node-4', 'node-5'): 15,

            }
        )
