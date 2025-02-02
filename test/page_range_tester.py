# To get the files from lstore
import sys

from lstore.page_range import PageRange
import lstore.config as config

sys.path.append('../lstore')

import unittest


class MyTestCase(unittest.TestCase):
    def test_write_record_and_capacity(self):
      test_page_range = PageRange(2)
      # This test is to make sure we can fill up the page range with the maximum amount of page records
      # 65536 = 4096 * 16 = basepage slots * number of basepages
      for i in range(65536):
        self.assertTrue(test_page_range.has_base_page_capacity())
        test_page_range.write_base_record([i, i, i, i, i, i])

      self.assertFalse(test_page_range.has_base_page_capacity())

    def test_read_base_record(self):
      test_page_range = PageRange(2)
      # This test is to make sure we can fill up the page range with the maximum amount of page records
      # 65536 = 4096 * 16 = basepage slots * number of basepages
      for i in range(65536):
        index, slot = test_page_range.write_base_record([i, i, i, i, i, i])
        self.assertEqual([i, i], test_page_range.read_base_record(index, slot, [1, 1])[4:])


if __name__ == '__main__':
    unittest.main()
