import datetime
from collections import defaultdict
from typing import Optional, Dict

import pandas as pd

from galileocontext.telemetry.api import Resources


def get_container_cpu(container_resources: Dict[str, Resources], container_id: str, cores: int,
                      start: datetime.datetime = None, end: datetime.datetime = None, absolute=True) -> \
        Optional[pd.DataFrame]:
    data = defaultdict(list)
    resource_windows = container_resources.get(container_id, None)
    if resource_windows is None:
        return None
    l = resource_windows.get_resource_windows("kubernetes_cgrp_cpu")
    if l is None:
        return None
    for w in l:
        data["container_id"].append(container_id)
        data["node"].append(resource_windows.node)
        data["ts"].append(w.ts)
        data["value"].append(w.val)
    df = pd.DataFrame(data=data,
                      index=pd.DatetimeIndex(pd.to_datetime(data["ts"], unit='s', origin='unix')))
    df = df.sort_values(by='ts')
    if len(l) >= 2:
        d = (l[1].ts - l[0].ts)
        # scale to 100%
        d_absolute = d * cores

        df['percentage'] = (df['value'].diff() / (d * 1e6)) / 10
        df['percentage_relative'] = (df['value'].diff() / (d_absolute * 1e9)) * 100
        df['value_ms'] = df['value'] / 1e6
        df['milli_cores'] = (df['value_ms'].diff() / d)
        df['percentage'] = df['milli_cores'] / 10

        # between 0 and 1
        df['percentage_relative'] = df['milli_cores'] / (10 * (cores * 100))

        # df['milli_cores_relative'] = (df['value_ms'].diff() / d_absolute)
        if start is None and end is None:
            return df
        if start is None:
            start = datetime.datetime(1970, 1, 1)
        if end is None:
            end = datetime.datetime.now()

        df = df.loc[start: end]
        return df
    else:
        return None


def get_node_cpu(node_resources: Dict[str, Resources], node: str):
    data = defaultdict(list)
    resource_windows = node_resources.get(node, None)
    if resource_windows is None:
        return None
    l = resource_windows.get_resource_windows("cpu")
    for w in l:
        data["node"].append(node)
        data["ts"].append(w.ts)
        data["value"].append(w.val)
    df = pd.DataFrame(data=data,
                      index=pd.DatetimeIndex(pd.to_datetime(data["ts"], unit='s', origin='unix')))
    return df
