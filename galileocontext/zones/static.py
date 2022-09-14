from typing import List

from galileocontext.zones.api import ZoneService


class StaticZoneService(ZoneService):

    def __init__(self, zones: List[str]):
        self.zones = zones

    def get_zones(self) -> List[str]:
        return self.zones
