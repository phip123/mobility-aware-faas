import abc


class NetworkService(abc.ABC):

    def get_latency(self, from_node: str, to_node: str) -> float:
        raise NotImplementedError()

    def get_max_latency(self) -> float:
        raise NotImplementedError()

    def update_latency(self, from_node: str, to_noe: str, value: float):
        raise NotImplementedError()
