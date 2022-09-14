import json
import logging
import os
import threading
from typing import Dict, List, Optional

from galileocontext.connections import RedisClient
from galileocontext.constants import pod_not_running, pod_running, pod_pending
from galileocontext.nodes.api import NodeService, Node
from galileocontext.nodes.static import StaticNodeService
from galileocontext.pod.api import PodContainer, PodService, parse_pod_container
from galileocontext.util.rwlock import ReadWriteLock

logger = logging.getLogger(__name__)


class RedisPodService(PodService):

    def __init__(self, rds_client: RedisClient, node_service: NodeService):
        super().__init__()
        self.node_service = node_service
        self.rds_client = rds_client
        self._pods: Dict[str, PodContainer] = {}
        self.rw_lock = ReadWriteLock()
        self.t: threading.Thread = None

    def get_pod_container_by_name(self, name: str) -> Optional[PodContainer]:
        self.rw_lock.acquire_read()
        pods = list(filter(lambda p: p.name == name, self._pods.values()))
        pod = None
        if len(pods) == 1:
            pod = pods[0]
        self.rw_lock.release_read()
        return pod

    def get_pod_containers_of_deployment(self, name: str, running: bool = True, status: str = None) -> List[PodContainer]:
        self.rw_lock.acquire_read()
        logger.debug(f'get_pod_containers_of_deployment for {name}')
        containers = []
        for pod_container in self._pods.values():
            if running and pod_container.status != pod_running:
                continue
            if status is not None and pod_container.status != status:
                continue
            if name in pod_container.name:
                containers.append(pod_container)
        logger.debug(f'found {len(containers)} containers for {name}')
        self.rw_lock.release_read()
        return containers

    def get_pod_containers(self) -> List[PodContainer]:
        self.rw_lock.acquire_read()
        l = list(self._pods.values())
        self.rw_lock.release_read()
        return l

    def get_pod_container_with_ip(self, pod_ip: str, running: bool = True, status: str = None) -> Optional[PodContainer]:
        found = None
        self.rw_lock.acquire_read()
        for pod in self._pods.values():
            if running and pod.status != pod_running:
                continue
            if status is not None and pod.status != status:
                continue
            if pod.podIP == pod_ip:
                found = pod
                break
        self.rw_lock.release_read()
        return found

    def add_pod(self, pod: PodContainer):
        self.rw_lock.acquire_write()
        stored_pod = self._pods.get(pod.podUid, None)
        if stored_pod is not None:
            if stored_pod.status == pod_pending or stored_pod.status == pod_running:
                # only update pod in case it is pending or running, prevents from updating a not running to running pod
                logger.info(f"Update pod: {pod}")
                self._pods[pod.podUid] = pod
        else:
            logger.info(f"Create pod: {pod}")
            self._pods[pod.podUid] = pod
        self.rw_lock.release_write()

    def shutdown_pod(self, pod_uid: str):
        logger.info(f"Shutdown pod with ID: {pod_uid}")
        self.rw_lock.acquire_write()
        try:
            self._pods[pod_uid].status = pod_not_running
        except KeyError:
            logger.info(f'Wanted to shutdown non existing pod with ID "{pod_uid}"')
        finally:
            self.rw_lock.release_write()

    def find_pod_containers_with_labels(self, labels: Dict[str, str] = None, node_labels: Dict[str, str] = None,
                                        running: bool = True, status: str = None) -> \
            List[PodContainer]:
        if node_labels is None:
            node_labels = dict()
        # logger.debug(f"find containers with labels: {labels}, and node labels: {node_labels}")
        pods = []
        self.rw_lock.acquire_read()
        for pod in self._pods.values():
            if running and pod.status != pod_running:
                continue
            if status is not None and pod.status != status:
                continue
            matches = True
            if labels is not None:
                pod_labels = pod.labels
                matches = self.matches_labels(labels, pod_labels)
            node = self.node_service.find(pod.nodeName)
            if not matches or node is None:
                continue
            if node_labels is not None:
                matches = matches and self.matches_labels(node_labels, node.labels)
            if matches:
                pods.append(pod)
        self.rw_lock.release_read()
        return pods

    def matches_labels(self, labels, to_match):
        for label in labels:
            to_match_value = to_match.get(label, None)
            if to_match_value is None or to_match_value != labels[label]:
                return False
        return True

    def run(self):
        for event in self.rds_client.sub('galileo/events'):
            try:
                msg = event['data']
                logger.debug("Got message: %s", msg)
                split = msg.split(' ', maxsplit=2)
                event = split[1]
                if 'pod' in event:
                    try:
                        pod_container = parse_pod_container(split[2])
                        if pod_container is None:
                            logger.warning(f"Emitted pod container does not adhere to structure: {split[2]}")
                        else:
                            logger.info(f"Handler container event ({event}):  {pod_container.name}")
                            if event == 'pod/running':
                                logger.info(f"Set pod running: {pod_container.name}")
                                self.add_pod(pod_container)
                            elif event == 'pod/delete':
                                logger.info(f'Delete pod {pod_container.name}')
                                self.shutdown_pod(pod_container.podUid)
                            elif event == 'pod/create':
                                logger.info(f"create pod: {pod_container.name}")
                                self.add_pod(pod_container)
                            elif event == 'pod/pending':
                                logger.info(f"pending pod: {pod_container.name}")
                                self.add_pod(pod_container)
                            elif event == 'pod/shutdown':
                                logger.info(f"shutdown pod {pod_container.name}")
                                self.shutdown_pod(pod_container.podUid)
                            else:
                                logger.error(f'unknown pod event ({event}): {pod_container.name}')
                    except:
                        logger.error(f"error parsing container - {msg}")
                elif event == 'scale_schedule':
                    obj = json.loads(split[2])
                    if obj['delete'] is True:
                        name = obj['pod']['pod_name']
                        pod = self.get_pod_container_by_name(name)
                        logger.info(f'Got scale_schedule event. Delete pod {pod}')
                        self.shutdown_pod(pod.podUid)
                else:
                    # ignore - not of intereset
                    pass
            except Exception as e:
                logging.error(e)
            pass

    def get_pod_container_with_id(self, container_id: str) -> Optional[PodContainer]:
        logger.debug(f'find container with id: {container_id}')
        self.rw_lock.acquire_read()
        found = None
        for pod in self._pods.values():
            if pod.container_id == container_id:
                found = pod
                break
        self.rw_lock.release_read()
        return found

    def get_pod_containers_on_node(self, node: Node) -> List[PodContainer]:
        logger.debug(f'find containers on ode {node.name}')
        self.rw_lock.acquire_read()
        pods = list(filter(lambda p: p.nodeName == node.name, self._pods.values()))
        self.rw_lock.release_read()
        return pods

    def start(self):
        self.t = threading.Thread(target=self.run)
        self.t.start()
        return self.t

    def stop(self):
        if self.t is not None:
            self.t.join(5)


if __name__ == '__main__':
    logging.basicConfig(level=logging._nameToLevel[os.environ.get('galileo_context_logging', 'DEBUG')])
    rds_client = RedisClient.from_env()
    pod_manager = RedisPodService(rds_client, StaticNodeService(['zone-a'], [
        Node('a', 'a', 3, 3, 3, [], [], 'a', 'b', {}, 'zone-a', {})]))
    pod_manager.run()
