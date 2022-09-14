from lbopt.weights.api import WeightUpdate


class WeightPublisher:

    def publish(self, update: WeightUpdate):
        raise NotImplementedError()
