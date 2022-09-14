import time
import unittest

from galileocontext.connections import KubernetesClient
from schedulescaleopt.k8s.create import create_pod
from schedulescaleopt.k8s.delete import delete_pod


class KubernetesResultConsumerTest(unittest.TestCase):
    """
    These tests currently rely on a running Kubernetes Cluster.
    """

    def setUp(self) -> None:
        self.created_pods = []
        self.k8s_client = KubernetesClient.from_env()
        self.coreAPI = self.k8s_client.corev1_api

    def tearDown(self) -> None:
        for pod in self.created_pods:
            delete_pod(self.coreAPI, pod.metadata.name, pod.metadata.namespace)

    def test_create_pod(self):
        pod_name = 'test-1234'
        pod = create_pod(self.coreAPI, 'default', pod_name, 'container-name', 'nginx', {}, {})
        self.created_pods.append(pod)
        time.sleep(2)
        found = False
        pod_list = self.coreAPI.list_namespaced_pod('default')
        for pod in pod_list.items:
            if pod.metadata.name == pod_name:
                found = True
                break
        self.assertTrue(found)
