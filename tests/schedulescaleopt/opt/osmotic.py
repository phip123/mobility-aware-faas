import unittest

from schedulescaleopt.opt.osmotic import get_average_requests_over_replicas
from tests.lbopt.opt.mockcontext import MockContext


class OsmoticScalerSchedulerOptimizerTest(unittest.TestCase):

    def setUp(self) -> None:
        self.mock_ctx = MockContext()
        self.ctx = self.mock_ctx.create()

    def test_get_average_requests_over_replicas(self):
        avg = get_average_requests_over_replicas('resnet', self.ctx.trace_service.get_traces_api_gateway('node-3'))
        self.assertEqual(1.5, avg)
