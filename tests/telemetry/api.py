import unittest

from lbopt.telemetry.api import PointWindow, Point


class TestBoundedResourceWindowList(unittest.TestCase):

    def test_append(self):
        l: PointWindow[float] = PointWindow(5)
        l.append(Point(0, 0))
        l.append(Point(1, 1))
        l.append(Point(2, 2))
        l.append(Point(3, 3))
        l.append(Point(4, 4))
        l.append(Point(5, 5))

        self.assertEqual(l.size(), 5)

        l.append(Point(6, 6))

        self.assertEqual(l.size(), 5)
