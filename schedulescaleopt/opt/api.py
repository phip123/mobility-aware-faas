from dataclasses import dataclass
from typing import List, Dict

from dataclasses_json import dataclass_json

from galileocontext.context.cluster import Context


@dataclass_json
@dataclass
class ApiPressureResult:
    ts: float
    node: str
    zone: str
    # the following results are the mean over all deployments
    mean: float
    median: float
    std: float
    min: float
    max: float

@dataclass_json
@dataclass
class FunctionPressureResult:
    ts: float
    fn: str
    fn_zone: str
    client_zone: str
    mean: float
    std: float
    min: float
    max: float

@dataclass_json
@dataclass
class ClientPressureResult:
    ts: float
    client: str
    client_zone: str
    fn: str
    fn_zone:str
    value: float

@dataclass_json
@dataclass
class PressureReqResult:
    ts: float
    value: float
    weight: float
    utilization: float
    client: str
    fn: str
    gateway: str
    zone: str


@dataclass_json
@dataclass
class PressureLatencyRequirementResult:
    ts: float
    type: str
    value: float
    fn: str
    distance: float
    client: str
    required_latency: float
    gateway: str
    max_value: float
    scaled_value: float
    zone: str


@dataclass_json
@dataclass
class PressureDistResult:
    ts: float
    value: float
    distance: float
    client: str
    avg_distance: float
    gateway: str
    max_value: float
    scaled_value: float
    zone: str

@dataclass_json
@dataclass
class CpuUsageResult:
    ts: float
    fn: str
    running_pods: int
    cpu_mean: float
    zone: str

@dataclass_json
@dataclass
class PodRequest:
    ts: float
    pod_name: str
    container_id: str
    image: str
    labels: Dict[str, str]
    resource_requests: Dict[str, str]
    namespace: str = "default"


@dataclass_json
@dataclass
class ScaleScheduleEvent:
    ts: float
    fn: str
    pod: PodRequest
    origin_zone: str
    dest_zone: str
    delete: bool = False


class ScalerSchedulerResult:
    scale_schedule_events: List[ScaleScheduleEvent]




@dataclass_json
@dataclass
class OsmoticScalerSchedulerResult(ScalerSchedulerResult):
    api_pressures: List[ApiPressureResult]
    fn_pressures: List[FunctionPressureResult]
    client_pressures: List[ClientPressureResult]
    pressure_requests: List[PressureReqResult]
    pressure_latency_requirement_results: List[PressureLatencyRequirementResult]
    pressure_dist_results: List[PressureDistResult]
    pressure_latency_requirement_log_results: List[PressureLatencyRequirementResult]
    pressure_rtt_log_results: List[PressureLatencyRequirementResult]
    cpu_usage_results: List[CpuUsageResult]
    scale_schedule_events: List[ScaleScheduleEvent]

@dataclass_json
@dataclass
class HlpaDecision:
    ts: float
    fn: str
    duration_agg: float
    percentile: float
    target_duration: float
    base_scale_ratio: float
    running_pods: int
    pending_pods: int
    threshold_tolerance: float
    desired_replicas: int

@dataclass_json
@dataclass
class HpaDecision:
    ts: float
    fn: str
    cpu_mean: float
    recalculated_cpu_mean: float
    target_utilization: float
    base_scale_ratio: float
    running_pods: int
    pending_pods: int
    missing_cpu: int
    threshold_tolerance: float
    desired_replicas: int
    recalculated_desired_replicas: int

@dataclass_json
@dataclass
class HcpaDecision:
    ts: float
    fn: str
    zone: str
    cpu_mean: float
    recalculated_cpu_mean: float
    target_utilization: float
    base_scale_ratio: float
    running_pods: int
    pending_pods: int
    missing_cpu: int
    threshold_tolerance: float
    desired_replicas: int
    recalculated_desired_replicas: int

@dataclass_json
@dataclass
class HlcpaDecision:
    ts: float
    fn: str
    zone: str
    duration_agg: float
    percentile: float
    target_duration: float
    base_scale_ratio: float
    running_pods: int
    pending_pods: int
    threshold_tolerance: float
    desired_replicas: int

@dataclass_json
@dataclass
class HcpaScalerSchedulerResult(ScalerSchedulerResult):
    scale_schedule_events: List[ScaleScheduleEvent]
    decisions: List[HcpaDecision]

@dataclass_json
@dataclass
class HlcpaScalerSchedulerResult(ScalerSchedulerResult):
    scale_schedule_events: List[ScaleScheduleEvent]
    decisions: List[HlcpaDecision]

@dataclass_json
@dataclass
class HpaScalerSchedulerResult(ScalerSchedulerResult):
    scale_schedule_events: List[ScaleScheduleEvent]
    decisions: List[HpaDecision]


@dataclass_json
@dataclass
class HlpaScalerSchedulerResult(ScalerSchedulerResult):
    scale_schedule_events: List[ScaleScheduleEvent]
    decisions: List[HlpaDecision]

class ScalerSchedulerOptimizer:

    def run(self, ctx: Context) -> ScalerSchedulerResult:
        raise NotImplementedError()
