import unittest
import sys
sys.path.append("..") 

from src.main import add_numbers


#testing
class TestSumFunction(unittest.TestCase):
    def test_sum_positive_numbers(self):
        self.assertEqual(add_numbers(1, 2), 3)

    def test_sum_negative_numbers(self):
        self.assertEqual(add_numbers(-1, -2), -3)

    def test_sum_mixed_numbers(self):
        self.assertEqual(add_numbers(1, -2), -1)


if __name__ == "__main__":
    unittest.main()
