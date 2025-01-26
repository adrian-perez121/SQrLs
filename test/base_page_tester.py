import unittest
from lstore.base_page import BasePage

class MyTestCase(unittest.TestCase):
	def test_has_capacity(self):
		test_page = BasePage()
		for i in range(10):
			test_page.write(i)

	#def test_write(self):



if __name__ == '__main__':
	unittest.main()
