from dataclasses import dataclass

import numpy as np


@dataclass
class LogisticFunctionParameters:
    a: float  # max
    b: float  # bottom
    c: float  # growth
    d: float
    offset: float  # offset of midpoint (values > 0 increase the y-value of the midpoint)


def logistic_curve(x, a, b, c, d):
    """
    Logistic function with parameters a, b, c, d
    a is the curve's maximum value (top asymptote)
    b is the curve's minimum value (bottom asymptote)
    c is the logistic growth rate or steepness of the curve
    d is the x value of the sigmoid's midpoint
    """
    return ((a - b) / (1 + np.exp(-c * (x - d)))) + b
