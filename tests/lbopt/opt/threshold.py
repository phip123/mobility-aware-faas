import unittest

from lbopt.opt.threshold import ThresholdBasedWeightOptimizer, Threshold
from tests.lbopt.opt.mockcontext import MockContext


class ThresholdBasedWeightOptimizerTest(unittest.TestCase):

    def setUp(self) -> None:
        self.mock_ctx = MockContext()
        self.ctx = self.mock_ctx.create()

    def test_run(self):
        opt = ThresholdBasedWeightOptimizer({'resnet': Threshold(0.3, 100)})
        weights = opt.run(self.ctx)
        print(weights)
