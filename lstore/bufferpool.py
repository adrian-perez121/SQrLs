
import os
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) # maybe window/vsc issue?

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
    
    # assign and return
    def request_page(self):
        self.page_request_count += 1
        page = MemoryPage(self.page_request_count)
        self.memory_pages.append(page)
        return page
        
        #page.request_count += 1
        #pass
    
    def has_capacity(self):
        return len(self.memory_pages) < self.capacity
    
    # sim writing to disk
    def write_page(self, page):
        page.is_dirty = False
        file_path = os.path.join(self.dir, f"page_{page.position}.txt")
        with open(file_path, "w") as f:
            f.write(f"MemoryPage {page.position} saved. \n")
    
    def get_least_needed_page(self, memory_pages):
        sorted_pages = sorted(memory_pages, key=lambda page: page.request_count)
        stop_index = 0
        least_requests = 0
        for i, page in enumerate(sorted_pages):
            if i == 0:
                least_requests = page.request_count
            if page.request_count > least_requests:
                stop_index = i
                break
            
        sorted_pages = sorted(sorted_pages[:stop_index], key=lambda page: page.last_accessed)
        return None if not sorted_pages else sorted_pages[0]
            
    
    def evict_page(self, memory_pages):
        # If Page Dirty, write to Disk
        if not self.has_capacity():
            useless_page = self.get_least_needed_page(memory_pages)
            if useless_page is None:
                return
            if useless_page.is_dirty:
                self.write_page(useless_page)
            self.memory_pages.remove(useless_page) # remove evicted


    #def evict_page(self, memory_pages):
        # If Page Dirty, write to Disk
        #if not self.has_capacity():
            #useless_page = self.get_least_needed_page(memory_pages)
            #if useless_page.is_dirty:
                #self.write_page(useless_page)
                #if useless_page.pins:
                    # M3: Try to evict something else
                    #self.write_page(useless_page)
                    #memory_pages[useless_page.position] = MemoryPage(useless_page.position) # M3: SUBMIT / QUEUE IT / ON PINS = 0 -> RUN
                #else:
                    #self.write_page(useless_page)
                    #memory_pages[useless_page.position] = MemoryPage(useless_page.position)
            #self.memory_pages.remove(useless_page)
        #pass    
    
    def on_close(self, memory_pages):
        for page in memory_pages:
            if page.is_dirty:
                self.write_page(page)
            
            
            
            
    