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




if __name__ == '__main__':
  unittest.main()