# To get the files from lstore
import os
import sys
import shutil

from lstore.conceptual_page import ConceptualPage
import lstore.config as config

sys.path.append('../lstore')

import unittest


class MyTestCase(unittest.TestCase):

  def test_has_capacity(self):
    test_page = ConceptualPage(1)
    for i in range(4096):
      self.assertTrue(test_page.has_capacity())
      test_page.write_record([1, 1, 1, 1, 1])

    self.assertFalse(test_page.has_capacity())

  def test_write_record(self):
    test_conceptual_page = ConceptualPage(3)
    for i in range(4096):
      self.assertTrue(test_conceptual_page.has_capacity())
      # Seven columns long because meta data columns
      test_conceptual_page.write_record([i,i,i,i,i,i,i])
    self.assertFalse(test_conceptual_page.has_capacity())

    with self.assertRaises(IndexError):
      test_conceptual_page.write_record([1,1,1,1,1,1,1])
    with self.assertRaises(Exception):
      test_conceptual_page.write_record([1])

  def test_read_record_at(self):
    test_conceptual_page = ConceptualPage(3)
    for i in range(4096):
      test_conceptual_page.write_record([0, 0, 0, 0, i, i, i])
      self.assertEqual([i,i,i], test_conceptual_page.read_record_at(i, [1,1,1]))
      self.assertEqual([i, None, i], test_conceptual_page.read_record_at(i, [1, None, 1]))
      self.assertEqual([None, None, None], test_conceptual_page.read_record_at(i, [None, None, None]))

  def test_read_meta_data_at(self):
    test_conceptual_page = ConceptualPage(4)
    for i in range(4096):
      test_conceptual_page.write_record([i, i, i, i, 0, 0, 0, 0])
      self.assertEqual([i, i, i, i], test_conceptual_page.read_metadata_at(i))

  def test_update_column(self):
    test_page = ConceptualPage(1)
    for i in range(4096):
      test_page.write_record([i,i,i,i,i])
      self.assertEqual(i, test_page.read_metadata_at(i)[config.INDIRECTION_COLUMN])
      test_page.update_column(config.INDIRECTION_COLUMN ,i, i + 1)
      self.assertEqual(i + 1, test_page.read_metadata_at(i)[config.INDIRECTION_COLUMN])

  def test_load_and_dump_files(self):
    test_conceptual_page = ConceptualPage(2)

    for i in range(0,4096, 5): # Step is to make the test faster

      # six columns long because meta data columns
      test_conceptual_page.write_record([i, i, i, i, i, i])
      test_conceptual_page.dump_file("testpage")

      loaded_page = ConceptualPage.load_file("testpage")

      # Make sure the physical pages match
      for i, column in enumerate(test_conceptual_page.pages):
        for j, physical_page in enumerate(column):
          self.assertEqual(physical_page.num_records, loaded_page.pages[i][j].num_records)
          self.assertEqual(physical_page.data, loaded_page.pages[i][j].data)

      self.assertEqual(test_conceptual_page.num_records, loaded_page.num_records)

    os.remove("testpage.json") # clean up






if __name__ == '__main__':
  unittest.main()
