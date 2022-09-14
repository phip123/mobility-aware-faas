from schedulescaleopt.opt.api import ScalerSchedulerResult


class ResultConsumer:

    def consume(self, scaler: ScalerSchedulerResult):
        raise NotImplementedError()
