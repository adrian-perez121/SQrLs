# To get the files from lstore
import sys

from lstore.conceptual_page import ConceptualPage

sys.path.append('../lstore')

import unittest
from conceptual_page import ConceptualPage


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


if __name__ == '__main__':
  unittest.main()
