import os
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  # Ensure module resolution

from lstore.page import Page
from lstore.conceptual_page import ConceptualPage


class MemoryPage:

    def __init__(self, position: int, from_disk: bool = False):
        self.position = position
        self.is_dirty = not from_disk  # Pages not read from disk should be dirty so that they are written
        self.request_count = 0
        self.last_accessed = time.time()
        self.base_page = Page()  # ensuree always a base page
        self.tail_page = None  # tail are optional

    def mark_accessed(self):
        self.last_accessed = time.time()
        self.request_count += 1


class BufferPool:

     # Memory Page
        # self.request_count
        # self.last_access

    def __init__(self, dir):
        self.page_request_count = 0  # For Most Used
        self.memory_pages = []
        self.capacity = 16  # M3: Do we need more frames?
        self.dir = dir
        self.next_file_name = 0
        self.files = {file: os.path.join(dir, file) for file in os.listdir(dir)}

        if not os.path.exists(dir):
            os.makedirs(dir)  # check if dir exists!

    def request_record(self):
        pass

    # load from disk or make new
    def read_page(self, position):
        file_path = os.path.join(self.dir, f"page_{position}.bin")

        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                data = bytearray(f.read())  # load bytes
            page = Page()  # create new instance
            page.data = data  # assign
            print(f"Loaded page {position} from disk.")
        else:
            page = Page()
            print(f"Created new page at position {position}.")

        memory_page = MemoryPage(position, from_disk=os.path.exists(file_path))
        memory_page.base_page = page
        self.memory_pages.append(memory_page)

        return memory_page

    def request_page(self, position=None):
        if position is None:
            position = self.page_request_count
            self.page_request_count += 1

        # evict pages if needed before adding
        while not self.has_capacity():
            print("Buffer is full! Evicting a page before requesting a new one...")
            self.evict_page()

        # check if page already in mem
        for page in self.memory_pages:
            if page.position == position:
                page.mark_accessed()
                return page

        # or read from disk / create new
        return self.read_page(position)
    
    def has_capacity(self):
        return len(self.memory_pages) < self.capacity

    # write to disk
    def write_page(self, memory_page):
        if not isinstance(memory_page, MemoryPage):
            raise TypeError(f"Invalid MemoryPage Type: Received {type(memory_page)}")

        if memory_page.base_page is None:
            print(f"Warning: MemoryPage {memory_page.position} has no base_page! Voiding write.")
            return

        file_path = os.path.join(self.dir, f"page_{memory_page.position}.bin")

        try:
            with open(file_path, "wb") as f:
                f.write(memory_page.base_page.data)

            memory_page.is_dirty = False  # mark clean after w
            print(f"Page {memory_page.position} has successfully been written to disk!")

        except Exception as e:
            print(f"Failed writing page {memory_page.position}: {e}")

    # based on req count / last access time
    def get_least_needed_page(self):
        if not self.memory_pages:
            return None
        return min(self.memory_pages, key=lambda page: (page.request_count, page.last_accessed))

    # removes least-needed page from mem
    def evict_page(self):
        if not self.memory_pages:
            print("No pages to evict!")
            return

        page_to_evict = self.get_least_needed_page()
        if page_to_evict is None:
            return

        # ensure dirty pages are written before evicting
        if page_to_evict.is_dirty:
            print(f"Writing dirty page {page_to_evict.position} before eviction...")
            self.write_page(page_to_evict)

        print(f"Evicted page {page_to_evict.position} to maintain capacity.")
        self.memory_pages.remove(page_to_evict)

    def on_close(self):
        for page in self.memory_pages[:]:  # iterate a copy to avoid errors from modifying
            if page.is_dirty:
                print(f"Writing dirty page {page.position} before shutdown...")
                self.write_page(page)
                page.is_dirty = False  # mark clean

        print("All pages saved. Buffer pool shutting down.")
