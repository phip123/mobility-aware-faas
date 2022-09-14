from typing import Tuple, Dict

from galileocontext.pod.api import PodService


def update_latencies(client_node: str, sent: float, headers: Dict[str, str], pod_service: PodService) -> Dict[
    Tuple[str, str], float]:
    """
    Updates the latencies based on the data in headers.
    First reads X-Forwarded-For to get all nodes in the request trace, and then updates the latency between each node.
    Starting with the client-gateway connection.
    """

    def find_ts(node: str, headers: Dict[str, str]) -> float:
        node_key = f'x-forwarded-host-{node}'
        for key in headers.keys():
            if key.lower() == node_key:
                return float(headers[key])
        return -1

    headers = headers.copy()
    forwarded_for = [x.replace(' ', '') for x in headers['X-Forwarded-For'].split(',')]

    # add client at the beginning
    forwarded_for = [client_node] + forwarded_for
    headers[f'X-Forwarded-Host-{client_node}'] = str(sent)

    # find node of pod that processed the request
    # "X-Final-Host": "10.0.3.1:8080, 10.42.33.30:8080"
    ip = [x.replace(' ', '') for x in headers['X-Final-Host'].split(',')][-1]
    pod = pod_service.get_pod_container_with_ip(ip)
    if pod is not None:
        pod_node = pod.nodeName
        forwarded_for = forwarded_for + [pod_node]
        headers[f'X-Forwarded-Host-{pod_node}'] = headers['X-Start']

    data = {}
    for i in range(len(forwarded_for) - 1):
        from_node = forwarded_for[i]
        from_ts = find_ts(from_node, headers)
        to_node = forwarded_for[i + 1]
        to_ts = find_ts(to_node, headers)
        # ts are in seconds, but latency is in ms
        # two times because latency is back and forth
        diff = (1000 * (to_ts - from_ts)) * 2
        data[(from_node, to_node)] = diff

    return data
