import abc
from dataclasses import dataclass
from typing import List

from galileocontext.context.cluster import Context
from lbopt.weights.api import WeightUpdate

@dataclass
class Threshold:
    cpu: float
    memory: float

class WeightOptimizer(abc.ABC):

    def run(self, ctx: Context) -> List[List[WeightUpdate]]:
        """
        Calculates the weights for each deployment and for each zone in the context
        :return:
        """
        raise NotImplementedError()
