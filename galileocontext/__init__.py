from typing import TypeVar, Generic, List

T = TypeVar('T')


class Point(Generic[T]):
    ts: float
    val: T

    def __init__(self, ts: float, val: T):
        self.ts = ts
        self.val = val

    def get_ts(self) -> float:
        return self.ts

    def get_value(self) -> T:
        return self.val

    def __repr__(self):
        return f"Window(ts={self.ts}, val={self.val})"


S = TypeVar('S')


class PointWindow(Generic[S]):
    _content: List[Point[S]]

    def __init__(self, limit: int):
        """

        :param limit: specifies the time window in seconds of content stored. I.e, if limit == 15, the list contains the
        last 15 seconds
        """
        self._content = []
        self.limit = limit

    def append(self, resource_window: Point[S]):
        self._content.append(resource_window)
        # not needed because old requests are filtered through dataframe
        # self._content.sort(key=lambda x: x.ts, reverse=False)
        # diff = resource_window.ts - self.limit
        # first_ts = self._content[0].ts
        # if first_ts <= diff:
        #     self._cleanup(diff)

    def _cleanup(self, stop: float):
        index = 0
        for i, window in enumerate(self._content):
            if window.ts >= stop:
                index = i
                break
        self._content = self._content[index + 1:]

    def size(self) -> int:
        return len(self._content)

    def value(self) -> List[Point[S]]:
        return list(self._content)
