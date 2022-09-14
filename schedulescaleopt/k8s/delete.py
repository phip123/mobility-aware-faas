import logging
import os
import threading
import uuid
from typing import Dict, Union

from kubernetes.client import V1ResourceRequirements, V1NodeSelector, V1NodeSelectorTerm, V1NodeSelectorRequirement

from galileocontext.constants import function_label, worker_role_label, zone_label
from schedulescaleopt.opt.api import PodRequest

logger = logging.getLogger(__name__)

from kubernetes import client


def delete_pod(v1: client.CoreV1Api, name: str, namespace: str):
    async_req = os.getenv("schedule_scale_opt_async_pod_creation", "False")
    async_req = async_req == "True"
    return v1.delete_namespaced_pod(name, namespace, async_req=async_req)
