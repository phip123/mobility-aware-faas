import abc
from typing import List


class ZoneService(abc.ABC):

    def get_zones(self) -> List[str]:
        raise NotImplementedError()