import abc
from dataclasses import dataclass
from typing import List, Dict

import dataclasses_json
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class Weights:
    ips: List[str]
    weights: List[int]


@dataclass_json
@dataclass
class WeightUpdate:
    ts: float
    fn: str
    zone: str
    weights: Weights


@dataclass
class FunctionState:
    """Stores for each zone, all functions and there associated weights"""
    functions: Dict[str, Dict[str, Weights]]


class WeightManager(abc.ABC):

    def read_state(self) -> FunctionState:
        raise NotImplementedError()

    def update(self, weight_update: WeightUpdate):
        raise NotImplementedError()
