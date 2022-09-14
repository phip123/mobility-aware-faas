import logging
import os
import threading
from typing import Generator, Dict, Optional

import etcd3
import pandas as pd
import redis
from influxdb_client import InfluxDBClient, WriteApi, QueryApi
from kubernetes import client, config

logger = logging.getLogger(__name__)


class EtcdClient:
    _etcd_client: etcd3

    def __init__(self, etcd_host: str, etcd_port: int, cluster_id: str):
        self.etcd_host = etcd_host
        self.etcd_port = etcd_port
        self._etcd_client = etcd3.client(host=etcd_host, port=etcd_port)

    @staticmethod
    def from_env() -> 'EtcdClient':
        etcd_host = os.environ.get('osmotic_lb_opt_etcd_host', 'localhost')
        etcd_port = int(os.environ.get('osmotic_lb_opt_etcd_port', 2379))
        expected_cluster_id = os.environ.get('osmotic_lb_opt_expected_cluster_id')
        logger.info(f"Connect to etcd instance {etcd_host}:{etcd_port}")
        return EtcdClient(etcd_host, etcd_port, expected_cluster_id)

    def write(self, key: str, value: str):
        self._etcd_client.put(key, value)
        # url = f'http://{self.etcd_host}:{self.etcd_port}/v3/keys/{key}'
        # data = value
        # http = urllib3.PoolManager()
        # req = http.request('PUT', url, body=data)
        # logger.debug(req)


class InfluxClient:
    db_client: InfluxDBClient
    write_api: WriteApi
    query_api: QueryApi

    def __init__(self, url: str, token: str, org: str, timeout: int):
        self.url = url
        self.token = token
        self.org = org
        self.db_client = InfluxDBClient(url=url, token=token, org=org, timeout=timeout)
        self.write_api = self.db_client.write_api()
        self.query_api = self.db_client.query_api()

    def close(self):
        """Close clients after terminate a script.

        :return: nothing
        """
        if self.write_api is not None:
            self.write_api.close()

        if self.db_client is not None:
            self.db_client.close()

    @staticmethod
    def from_env() -> 'InfluxClient':
        url = os.environ.get('osmotic_lb_opt_expdb_influxdb_url', 'http://localhost:8086')
        token = os.environ.get('osmotic_lb_opt_expdb_influxdb_token', 'token')
        org = os.environ.get('osmotic_lb_opt_expdb_influxdb_org', 'org')
        timeout = int(os.environ.get('osmotic_lb_opt_expdb_influxdb_timeout', 10_000))
        return InfluxClient(url, token, org, timeout)

    def query_csv(self, query: str) -> pd.DataFrame:
        return self.query_api.query_data_frame(query, org=self.org)


class KubernetesClient:

    def __init__(self, load_config: str):
        self.load_config = load_config
        self._core_v1_api = None
        self._init_kubernetes()

    def _init_kubernetes(self):
        """
        Reads from env variables from where the config should be loaded and creates an API object.
        Defaults to local config.
        Env variable: schedule_scale_opt_k8s_config, possible values: local, in_cluster
        :return: instantiated CoreV1API object
        """

        self._load_kube_config()

        logger.info("Creating Kubernetes API objects")
        self._core_v1_api = client.CoreV1Api()
        logger.info("Created Kubernetes API objects")

    def _load_kube_config(self):
        # https://kubernetes.io/docs/tasks/run-application/access-api-from-pod/#using-official-client-libraries
        # https://github.com/kubernetes-client/python/blob/master/examples/in_cluster_config.py
        load_config = self.load_config
        logger.info(f"Loading {load_config} Kubernetes Config")
        if load_config == "local":
            config.load_kube_config()
        elif load_config == "in_cluster":
            config.load_incluster_config()
        else:
            logger.error(f"Invalid kubernetes config place: {load_config}")
            exit(1)
        logger.info(f"Loaded {load_config} Kubernetes Config")

    @property
    def corev1_api(self) -> Optional[client.CoreV1Api]:
        return self._core_v1_api

    @staticmethod
    def from_env() -> 'KubernetesClient':
        load_config = os.environ.get("schedule_scale_opt_k8s_config", "local")
        return KubernetesClient(load_config)


class RedisClient:
    _rds: redis.Redis

    def __init__(self, host: str, port: int, password: str = None, timeout: float = None):
        self.host = host
        self.port = port
        self._rds = redis.Redis(host, port, password=password, decode_responses=True, socket_timeout=timeout)

    def conn(self) -> redis.Redis:
        return self._rds

    def close(self):
        self._rds.close()

    def sub(self, key: str) -> Generator[Dict, None, None]:
        sub = None
        try:
            sub = self._rds.pubsub(ignore_subscribe_messages=True)
            sub.subscribe(key)
            yield from sub.listen()
        finally:
            if sub is not None:
                sub.close()

    @staticmethod
    def from_env() -> 'RedisClient':
        host = os.environ.get('galileo_context_redis_host', 'localhost')
        password = os.environ.get('galileo_context_redis_password', None)
        port = int(os.environ.get('galileo_context_redis_port', 6379))
        client = RedisClient(host, port, password, 4)
        client.conn().get('test')
        client.close()
        client = RedisClient(host, port, password)
        return client

    def psub(self, pattern: str) -> Generator[Dict, None, None]:
        sub = None
        try:
            sub = self._rds.pubsub(ignore_subscribe_messages=True)
            sub.psubscribe(pattern)
            yield from sub.listen()
        except Exception as e:
            logger.warning("Redis pubsub got error", e)
        finally:
            if sub is not None:
                sub.close()

    def publish_async(self, ch: str, msg: str) -> threading.Thread:
        def run():
            self.conn().publish(ch, msg)

        self.conn().publish(ch, msg)
        return None

    def ping(self):
        self._rds.ping()
