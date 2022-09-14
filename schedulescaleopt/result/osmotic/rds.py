import logging
from typing import List

from galileocontext.connections import RedisClient
from schedulescaleopt.opt.api import ApiPressureResult, PressureReqResult, \
    PressureLatencyRequirementResult, PressureDistResult, ScaleScheduleEvent, ClientPressureResult, \
    FunctionPressureResult, OsmoticScalerSchedulerResult
from schedulescaleopt.result.api import ResultConsumer

logger = logging.getLogger(__name__)


class RedisOsmoticResultConsumer(ResultConsumer):

    def __init__(self, rds: RedisClient, channel: str = 'galileo/events'):
        self.rds = rds
        self.channel = channel

    def _publish(self, msgs: List, name, clz):
        for msg in msgs:
            logger.debug(msg)
            ts = msg.ts
            rds_msg = f'{ts} {name} {clz.to_json(msg)}'
            self.rds.publish_async(self.channel, rds_msg)

    def consume(self, scaler: OsmoticScalerSchedulerResult):
        logger.info("redis result consumer is publishing the results...")

        self._publish(scaler.api_pressures, 'api_pressure', ApiPressureResult)
        self._publish(scaler.pressure_requests, 'pressure_req', PressureReqResult)
        self._publish(scaler.pressure_latency_requirement_results, 'pressure_latency_req',
                      PressureLatencyRequirementResult)
        self._publish(scaler.fn_pressures, 'pressure_fn', FunctionPressureResult)
        self._publish(scaler.client_pressures, 'pressure_client', ClientPressureResult)
        self._publish(scaler.pressure_dist_results, 'pressure_dist', PressureDistResult)
        self._publish(scaler.scale_schedule_events, 'scale_schedule', ScaleScheduleEvent)
        self._publish(scaler.cpu_usage_results, 'cpu_usage_pressure', ScaleScheduleEvent)
        self._publish(scaler.pressure_latency_requirement_log_results,
                      'pressure_latency_req_log', PressureLatencyRequirementResult)
        self._publish(scaler.pressure_rtt_log_results, 'pressure_rtt_log', PressureLatencyRequirementResult)


if __name__ == '__main__':
    rds = RedisClient.from_env()
    c = RedisOsmoticResultConsumer(rds)
    c.consume(OsmoticScalerSchedulerResult(
        [ApiPressureResult(node='a', zone='b', value=3)],
        [],
        [],
        [],
        []
    ))
