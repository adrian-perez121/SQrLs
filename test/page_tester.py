# To get the files from lstore
import sys
sys.path.append('../lstore')

import unittest
from page import Page

class MyTestCase(unittest.TestCase):
  def test_has_capacity(self):
    test_page = Page()
    for i in range(512):
      self.assertEqual(test_page.has_capacity(), True)
      test_page.write(i, i)
    self.assertEqual(test_page.has_capacity(), False)

  def test_read(self):
    test_page = Page()
    # Write in and immediately read
    for i in range(512):
      test_page.write(i,i)
      self.assertEqual(test_page.read(i), i)

  def test_read_exceptions(self):
    test_page = Page()
    with self.assertRaises(IndexError):
      test_page.read(-1)
      test_page.read(513)
      test_page.read(10)

  def test_to_and_from_dict(self):
    test_page = Page()
    for i in range(512):
      test_page.write(i,i)
      data = test_page.to_dict()
      new_page = Page.from_dict(data)
      self.assertEqual(test_page.num_records, new_page.num_records)
      self.assertEqual(test_page.data, new_page.data)




if __name__ == '__main__':
  unittest.main()
