
import os
import time

from lstore.conceptual_page import ConceptualPage

class MemoryPage: # / Frame
    def __init__(self, position: int, from_disk: bool = False):
        self.is_dirty: bool = not from_disk # Pages not read from disk should be dirty so that they are written
        self.request_count: int = 0
        self.last_accessed = time.time()
        self.position: int = position
        self.base_page: ConceptualPage = None
        self.tail_page: ConceptualPage = None

class BufferPool:
    
    # Memory Page
        # self.request_count
        # self.last_access
    
    def __init__(self, dir):
        self.page_request_count = 0 # For Most Used
        self.memory_pages: list[MemoryPage] = []
        self.capacity = 16 # M3: Do we need more frames?
        self.dir = dir
        self.next_file_name = 0
        self.files = {file: os.path.join(dir, file) for file in os.listdir(dir)}
        pass
    
    def request_record():
        pass
    
    def request_page(self):
        self.page_request_count += 1
        page: MemoryPage = None
        
        page.request_count += 1
        pass
    
    def has_capacity():
        pass
    
    def write_page():
        pass
    
    def get_least_needed_page(self, memory_pages):
        sorted_pages = sorted(memory_pages, key=MemoryPage.request_count)
        stop_index = 0
        least_requests = 0
        for i, page in enumerate(sorted_pages):
            if i == 0:
                least_requests = page.request_count
            if page.request_count > least_requests:
                stop_index = i
                break
            
        sorted_pages = sorted(sorted_pages[0:stop_index], key=MemoryPage.last_accessed)
        return None if not sorted_pages else sorted_pages[0]
            
    
    def evict_page(self, memory_pages):
        # If Page Dirty, write to Disk
        if memory_pages and not self.has_capacity():
            useless_page = self.get_least_needed_page(memory_pages)
            if useless_page.is_dirty:
                
                if useless_page.pins:
                    # M3: Try to evict something else
                    self.write_page(useless_page)
                    memory_pages[*useless_page.position] = MemoryPage() # M3: SUBMIT / QUEUE IT / ON PINS = 0 -> RUN
                else:
                    self.write_page(useless_page)
                    memory_pages[*useless_page.position] = MemoryPage()
        pass
    
    def on_close(self, memory_pages):
        for page in memory_pages:
            if page.is_dirty:
                self.write_page(page)
            
            
            
            
    