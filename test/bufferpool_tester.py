import sys
import os
import unittest

sys.path.append("../lstore")  # Ensure lstore module is included

from lstore.bufferpool import BufferPool, MemoryPage

class BufferPoolTests(unittest.TestCase):
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

    def test_initial_capacity(self):
        self.assertTrue(self.buffer_pool.has_capacity())

    def test_request_page(self):
        page = self.buffer_pool.request_page()
        self.assertIsInstance(page, MemoryPage)
        self.assertEqual(page.request_count, 0)

    def test_write_page(self):
        page = MemoryPage(0)
        page.is_dirty = True
        self.buffer_pool.write_page(page)
        print(f"Page {page.position} has successfully been written on disk!")
        self.assertFalse(page.is_dirty)

    def test_evict_page(self):
        for i in range(self.buffer_pool.capacity):
            self.buffer_pool.memory_pages.append(MemoryPage(i))
        self.buffer_pool.evict_page(self.buffer_pool.memory_pages)
        self.assertLessEqual(len(self.buffer_pool.memory_pages), self.buffer_pool.capacity)

    def test_least_needed_page(self):
        page1 = MemoryPage(0)
        page2 = MemoryPage(1)
        page3 = MemoryPage(3)
        page1.request_count = 5
        page2.request_count = 2
        page3.request_count = 1
        self.buffer_pool.memory_pages.extend([page1, page2, page3])
        least_needed_page = self.buffer_pool.get_least_needed_page(self.buffer_pool.memory_pages)
        self.assertEqual(least_needed_page, page3)

    def test_on_close(self):
        page = MemoryPage(0)
        page.is_dirty = True
        self.buffer_pool.memory_pages.append(page)
        self.buffer_pool.on_close(self.buffer_pool.memory_pages)
        for p in self.buffer_pool.memory_pages:
            self.assertFalse(p.is_dirty)

if __name__ == '__main__':
    unittest.main()
