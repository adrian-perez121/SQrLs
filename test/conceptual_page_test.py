# To get the files from lstore
import sys

from lstore.conceptual_page import ConceptualPage

sys.path.append('../lstore')

import unittest
from conceptual_page import ConceptualPage


class MyTestCase(unittest.TestCase):

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


  # def test_has_capacity(self):
  #   test_page = ConceptualPage(1)
  #   for i in range(512):
  #     self.assertTrue(test_page.has_capacity())
  #     test_page.new_record([1])
  #
  #   self.assertFalse(test_page.has_capacity())


if __name__ == '__main__':
  unittest.main()
