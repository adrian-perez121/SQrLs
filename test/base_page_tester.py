# To get the files from lstore
import sys
sys.path.append('../lstore')

import unittest
from base_page import BasePage


class MyTestCase(unittest.TestCase):
	def test(self):
		test_page = BasePage(5)
		for i in range(5):
			test_page.new_record([123,123,123,123,123])
			record = test_page.get_record(i, [1,1,1,1,1])
			self.assertEqual(record, [123,123,123,123,123])

	#def test_write(self):



if __name__ == '__main__':
	unittest.main()
