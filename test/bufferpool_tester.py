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
        self.buffer_pool.evict_page()
        self.assertLessEqual(len(self.buffer_pool.memory_pages), self.buffer_pool.capacity)

    def test_least_needed_page(self):
        page1 = MemoryPage(0)
        page2 = MemoryPage(1)
        page3 = MemoryPage(3)
        page1.request_count = 5
        page2.request_count = 2
        page3.request_count = 1

        self.buffer_pool.memory_pages.extend([page1, page2, page3])

        least_needed_page = self.buffer_pool.get_least_needed_page()
        self.assertEqual(least_needed_page, page3)

    def test_frequent_page_requests(self):
        # req and access pages multiple times
        page1 = self.buffer_pool.request_page(1)
        page2 = self.buffer_pool.request_page(2)
        for _ in range(100):  # sim freq access
            page1.request_count += 1

        # fill to max
        for i in range(3, self.buffer_pool.capacity + 3):
            self.buffer_pool.request_page(i)
        # evict
        self.buffer_pool.evict_page()
        # if freq accessed paged is still in mem
        self.assertIn(page1, self.buffer_pool.memory_pages)

    def test_max_capacity_eviction(self):
        # fill bp
        for i in range(self.buffer_pool.capacity):
            self.buffer_pool.request_page(i)
        # req should evict last needed
        self.buffer_pool.request_page(self.buffer_pool.capacity + 1)

        # shouldn't exceed cap
        self.assertLessEqual(len(self.buffer_pool.memory_pages), self.buffer_pool.capacity)

    def test_write_and_reload_page(self):
        # create and write page
        page = self.buffer_pool.request_page(99)
        page.is_dirty = True
        self.buffer_pool.write_page(page)
        # clear mem
        self.buffer_pool.memory_pages.clear()
        # reload page from disk
        reloaded_page = self.buffer_pool.request_page(99)
        # check if loaded properly
        self.assertEqual(reloaded_page.position, 99)
        self.assertFalse(reloaded_page.is_dirty)  # should be clean after w

    def test_eviction_writes_dirty_pages(self):
        for i in range(self.buffer_pool.capacity):
            page = self.buffer_pool.request_page(i)
            page.is_dirty = True  # set all dirty

        # get pate to evict
        evicted_page = self.buffer_pool.get_least_needed_page()
        evicted_position = evicted_page.position

        self.buffer_pool.evict_page()

        # check if evicted was w to disk
        file_path = os.path.join(self.test_dir, f"page_{evicted_position}.bin")
        print(f"Checking if file {file_path} exists...")
        self.assertTrue(os.path.exists(file_path))

    def test_consecutive_evictions(self):
        # fill bp
        for i in range(self.buffer_pool.capacity):
            self.buffer_pool.request_page(i)
        # req multiple new pages, force multiple evicts
        for i in range(self.buffer_pool.capacity, self.buffer_pool.capacity + 5):
            self.buffer_pool.request_page(i)
        # only latest remain in mem
        remaining_positions = [page.position for page in self.buffer_pool.memory_pages]
        for old_page in range(5):  # 1st 5 should be evicted
            self.assertNotIn(old_page, remaining_positions, f"Page {old_page} should be evicted!")

    def test_on_close(self):
        page = MemoryPage(0)
        page.is_dirty = True
        self.buffer_pool.memory_pages.append(page)

        self.buffer_pool.on_close()

        for p in self.buffer_pool.memory_pages:
            self.assertFalse(p.is_dirty)

if __name__ == '__main__':
    unittest.main()
