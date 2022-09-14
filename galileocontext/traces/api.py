import abc

import pandas as pd

from galileocontext.deployments.api import Deployment


class TracesService(abc.ABC):

    def get_traces_api_gateway(self, hostname: str) -> pd.DataFrame:
        """
        Returns all traces that were processed in the region the passed api gateway is situated.
        Which means that the API gateway was the last one to forward the request to actual compute unit (i.e., Pod)

        DataFrame contains:
            'ts'
            'function'
            'image'
            'container_id'
            'node'
            'rtt'
            'sent'
            'done'
            'origin_zone'
            'dest_zone'
            'client

        :param hostname:
        :return: DataFrame containing the traces
        """
        raise NotImplementedError()

    def get_traces_for_deployment(self, deployment: Deployment, start, end, zone: str = None):
        """
        Returns all traces that were processed for the given deployment.

        DataFrame contains:
            'ts'
            'function'
            'image'
            'container_id'
            'node'
            'rtt'
            'sent'
            'done'
            'origin_zone'
            'dest_zone'
            'client

        :param hostname:
        :return: DataFrame containing the traces
        """
        raise NotImplementedError()

    def get_traces_for_fn(self, fn: str, start, end, zone: str = None):
        """
        Returns all traces that were processed for the given deployment.

        DataFrame contains:
            'ts'
            'function'
            'image'
            'container_id'
            'node'
            'rtt'
            'sent'
            'done'
            'origin_zone'
            'dest_zone'
            'client

        :param hostname:
        :return: DataFrame containing the traces
        """
        raise NotImplementedError()