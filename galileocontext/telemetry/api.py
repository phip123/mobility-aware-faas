import abc
import datetime
from typing import Optional, Dict, List

import pandas as pd

from galileocontext import PointWindow, Point
from galileocontext.util.rwlock import ReadWriteLock


class Resources:
    node: str
    limit: int
    window_lists: Dict[str, PointWindow[float]]

    def __init__(self, node: str, limit: int, metrics: List[str]):
        self.node = node
        self.limit = limit
        self.metrics = metrics
        self.window_lists = {}
        self.rw_locks = {}
        for metric in metrics:
            self.rw_locks[metric] = ReadWriteLock()
            self.window_lists[metric] = PointWindow[float](self.limit)

    def append(self, metric: str, resource_point: Point) -> bool:
        """
        Append the window to the specified metric
        :return: false if metric is not supported, otherwise true
        """
        window_list = self.window_lists.get(metric, None)
        if window_list is None:
            return False
        else:
            self.rw_locks[metric].acquire_write()
            window_list.append(resource_point)
            self.rw_locks[metric].release_write()
            return True

    def get_resource_windows(self, metric: str) -> Optional[List[Point[float]]]:
        window_list = self.window_lists.get(metric, None)
        if window_list is None:
            return None
        else:
            self.rw_locks[metric].acquire_read()
            ret = window_list.value()
            self.rw_locks[metric].release_read()
            return ret


class TelemetryService(abc.ABC):

    def get_container_cpu(self, container_id: str, start: datetime.datetime = None, end: datetime.datetime = None) -> Optional[pd.DataFrame]:
        """
        Fetch the measured cpu times for the given container in the given timeframe.
        If start is None, the start of the selection will be set to 01-01-1970.
        If end is None, the end will be set to now.
        """
        raise NotImplementedError()

    def get_node_cpu(self, node: str) -> Optional[pd.DataFrame]:
        raise NotImplementedError()
