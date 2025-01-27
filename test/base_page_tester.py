# To get the files from lstore
import sys

sys.path.append('../lstore')

import unittest
from base_page import BasePage


class MyTestCase(unittest.TestCase):
  def test_new_record(self):
    test_page = BasePage(5)
    for i in range(5):
      test_page.new_record([123, 123, 123, 123, 123])
      record = test_page.get_record(i, [1, 1, 1, 1, 1])
      self.assertEqual(record, [123, 123, 123, 123, 123])

  def test_has_capacity(self):
    test_page = BasePage(1)
    for i in range(512):
      self.assertTrue(test_page.has_capacity())
      test_page.new_record([1])

    self.assertFalse(test_page.has_capacity())


if __name__ == '__main__':
  unittest.main()
