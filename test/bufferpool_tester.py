import sys
import os
import unittest

sys.path.append("../lstore")  # Ensure lstore module is included

from lstore.bufferpool import BufferPool, MemoryPage

class BufferPoolTestCase1(unittest.TestCase):
    # testing dir exists
    def setUp(self):
        self.test_dir = "test_dir"
        if not os.path.exists(self.test_dir):
            os.makedirs(self.test_dir)
        self.buffer_pool = BufferPool(self.test_dir)

    # clean after tests
    def tearDown(self):
        if os.path.exists(self.test_dir):
            for file in os.listdir(self.test_dir):
                os.remove(os.path.join(self.test_dir, file))
            os.rmdir(self.test_dir)

    def test_bufferpool_initialization(self):
        self.assertIsInstance(self.buffer_pool, BufferPool)

if __name__ == '__main__':
    unittest.main()
