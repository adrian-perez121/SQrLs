import os
import sys
import time
import pickle # for serialization

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
    
    def request_record(self):
        pass
    
    # load from disk or make new
    def read_page(self, position):
        file_path = os.path.join(self.dir, f"page_{position}.pkl")
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                page_data = pickle.load(f)  # load serialized page

            # mempage from stored attributes
            page = MemoryPage(position, from_disk=True)
            page.request_count = page_data["request_count"]
            page.last_accessed = page_data["last_accessed_page"]
            # make sure page is clean after writing to disk
            page.is_dirty = False  
            page.base_page = page_data["base_page"]
            page.tail_page = page_data["tail_page"]
            print(f"Loaded page {position} from disk (is_dirty={page.is_dirty})!")

        else:
            page = MemoryPage(position)
            print(f"Created new page at {position}.")

        self.memory_pages.append(page)
        return page

    # assign and return
    def request_page(self, position=None):
        if position is None:
            position = self.page_request_count
            self.page_request_count += 1

        # evcit before adding new
        self.evict_page()

        # check if in mem
        for page in self.memory_pages:
            if page.position == position:
                return page

        # otherwise read from disk
        return self.read_page(position)
    
    def has_capacity(self):
        return len(self.memory_pages) < self.capacity
    
    # write to disk
    def write_page(self, page):
        file_path = os.path.join(self.dir, f"page_{page.position}.pkl")
        
        # using page attributes, metadata + b/t page
        with open(file_path, "wb") as f:
            pickle.dump({
                "position": page.position,
                "request_count": page.request_count,
                "last_accessed_page": page.last_accessed,
                "is_dirty": page.is_dirty,
                "base_page": page.base_page,
                "tail_page": page.tail_page,
            }, f)
        page.is_dirty = False # clean
        print(f"Page {page.position} has successfully been written to disk!")
    
    def get_least_needed_page(self, memory_pages):
        if not memory_pages:
            return None
        return min(memory_pages, key=lambda page: (page.request_count, page.last_accessed))

    def evict_page(self):
        while not self.has_capacity():
            useless_page = self.get_least_needed_page(self.memory_pages)
            if useless_page is None:
                return  # none to evict
            if useless_page.is_dirty:
                self.write_page(useless_page)  # w before evict
            self.memory_pages.remove(useless_page)
            print(f"Evicted page {useless_page.position} to maintain capacity.")

    
    def on_close(self, memory_pages):
        for page in self.memory_pages[:]:
            if page.is_dirty:
                self.write_page(page)
            
            
            
            
    