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

    def test_write_and_read_tail_record(self):
      test_page_range = PageRange(2)
      for i in range(65536):
        index, slot = test_page_range.write_tail_record([i, i, i, i, i, i])
        self.assertEqual([i, i, i, i, None, i], test_page_range.read_tail_record(index, slot, [0, 1]))

    def test_read_base_record(self):
      test_page_range = PageRange(2)
      # This test is to make sure we can fill up the page range with the maximum amount of page records
      # 65536 = 4096 * 16 = basepage slots * number of basepages
      for i in range(65536):
        index, slot = test_page_range.write_base_record([i, i, i, i, i, i])
        self.assertEqual([i, i], test_page_range.read_base_record(index, slot, [1, 1])[4:])

    def test_update_for_both_records(self):
      test_page_range = PageRange(1)
      for i in range(8000):
        # In these tests we are going to be updating the indirection columns
        b_index, b_slot = test_page_range.write_base_record([i, i, i, i, i])
        self.assertEqual(i, test_page_range.read_base_record(b_index, b_slot, [0])[config.INDIRECTION_COLUMN])
        test_page_range.update_base_record_column(b_index, b_slot, config.INDIRECTION_COLUMN, i + 1)
        self.assertEqual(i + 1, test_page_range.read_base_record(b_index, b_slot, [0])[config.INDIRECTION_COLUMN])

        t_index, t_slot = test_page_range.write_tail_record([i, i, i, i, i])
        self.assertEqual(i, test_page_range.read_tail_record(t_index, t_slot, [0])[config.INDIRECTION_COLUMN])
        test_page_range.update_tail_record_column(b_index, b_slot, config.INDIRECTION_COLUMN, i + 1)
        self.assertEqual(i + 1, test_page_range.read_tail_record(t_index, t_slot, [0])[config.INDIRECTION_COLUMN])

    def test_to_and_from_dict(self):
        test_page_range = PageRange(1)
        for i in range(3000):
            b_index, b_slot = test_page_range.write_base_record([i, i, i, i, i])
            t_index, t_slot = test_page_range.write_tail_record([i, i, i, i, i])

            data = test_page_range.to_dict()
            new_page_range = PageRange.from_dict(data)

            b_record = test_page_range.read_base_record(b_index, b_slot, [1] * test_page_range.regular_columns)
            new_b_record = new_page_range.read_base_record(b_index, b_slot, [1] * test_page_range.regular_columns)
            self.assertEqual(b_record, new_b_record)

            t_record = test_page_range.read_tail_record(t_index, t_slot, [1] * test_page_range.regular_columns)
            new_t_record = new_page_range.read_tail_record(t_index, t_slot, [1] * test_page_range.regular_columns)
            self.assertEqual(t_record, new_t_record)

            # Since they are the same they should make the same dict
            self.assertEqual(test_page_range.to_dict(), new_page_range.to_dict())

if __name__ == '__main__':
    unittest.main()
