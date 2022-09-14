import unittest

from galileocontext import PointWindow, Point


class PointWindowTest(unittest.TestCase):

    def test_append(self):
        window = PointWindow(5,real_time=False)
        window.append(Point(1, 'a'))
        window.append(Point(2, 'b'))
        window.append(Point(3, 'c'))
        window.append(Point(4, 'd'))
        window.append(Point(5, 'e'))
        window.append(Point(6, 'f1'))
        window.append(Point(6, 'f2'))
        window.append(Point(6, 'f3'))
        window.append(Point(6, 'f4'))
        window.append(Point(7, 'i'))


        expected_values = ['c', 'd', 'e', 'f1', 'f2', 'f3', 'f4', 'i']
        self.assertTrue(window.size() == len(expected_values),
                        f'Size is not {len(expected_values)} but {window.size()}')

        mapped_values = list(map(lambda x: x.val, window.value()))
        self.assertEqual(expected_values, mapped_values)
