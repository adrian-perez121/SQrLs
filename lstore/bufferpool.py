
import os
import time
import json
import threading
from typing import Optional

from lstore.conceptual_page import ConceptualPage


class MemoryPage:
    """A representation of an in-memory frame storing base/tail pages."""

    def __init__(self, position: int, from_disk: bool = False):
        self.position = position
        self.is_dirty = not from_disk  # Pages not read from disk should be dirty so that they are written
        self.request_count = 0
        self.last_accessed = time.time()
        self.base_page = Page()  # ensuree always a base page
        self.tail_page = None  # tail are optional

    def mark_accessed(self):
        """Updates access time and request count."""
        self.last_accessed = time.time()
        self.request_count += 1


class BufferPool:
    
    # Memory Page
        # self.request_count
        # self.last_access
    
    def __init__(self, path):
<<<<<<< Updated upstream
        self.frame_request_count = 0 # For Most Used
        self.frames: dict[str, list[Frame]] = {} # When Frames are evicted, the frame in the list is replaced with None
        self.capacity = 16 # M3: Do we need more frames?
=======
        self.lock = threading.Lock() 
        self.frame_request_count = 0  # For Most Used
        self.frames: dict[str, list[Frame]] = {}
        self.capacity = 16  # M3: Do we need more frames?
>>>>>>> Stashed changes
        self.path = path
        self.next_file_name = 0
        self.files = {file: os.path.join(dir, file) for file in os.listdir(dir)}

        if not os.path.exists(dir):
            os.makedirs(dir)  # check if dir exists!

    def request_record(self):
        pass

<<<<<<< Updated upstream
    # load from disk or make new
    def read_page(self, position):
        file_path = os.path.join(self.dir, f"page_{position}.bin")

        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                data = bytearray(f.read())  # load bytes
            page = Page()  # create new instance
            page.data = data  # assign
            print(f"✅ Loaded page {position} from disk.")
        else:
            page = Page()
            print(f"📄 Created new page at position {position}.")

        memory_page = MemoryPage(position, from_disk=os.path.exists(file_path))
        memory_page.base_page = page
        self.memory_pages.append(memory_page)
=======
    def get_frame(self, table_name: str, page_range_index: int, num_columns: int):
        """Retrieves a frame from the buffer pool, or loads it from disk if needed."""
        self.frames.setdefault(table_name, [])

        if table_name in self.frames:
            frames: list[Frame] = self.frames[table_name]
            append_count = max(0, page_range_index - len(frames) + 1)
            frames.extend([None] * append_count)

            frame: Frame = frames[page_range_index]

            if frame is None:
                # 🛠 Fix: Read or Create a Frame if it doesn't exist
                self.read_frame(table_name, page_range_index, num_columns)
                frame = self.frames[table_name][page_range_index]

                # If it still doesn't exist, create a new one
                if frame is None:
                    print(f"⚠️ Creating new frame for {table_name} [{page_range_index}]")
                    frame = Frame(table_name, page_range_index, PageRange(num_columns))
                    self.frames[table_name][page_range_index] = frame

            # ✅ Now `frame` is guaranteed to exist
            frame.request_count += 1
            return frame
        else:
            print(f"❌ Error: Table {table_name} not found in frames!")
            return None


    def has_capacity(self):
        sum = 0
        for table_frames in self.frames.values():
            for frame in table_frames:
                if frame:
                    sum += 1
        
        return sum <= self.capacity
>>>>>>> Stashed changes

        return memory_page

    def request_page(self, position=None):
        """Returns a page, loading from disk if necessary."""
        if position is None:
            position = self.page_request_count
            self.page_request_count += 1

        # evict pages if needed before adding
        while not self.has_capacity():
            print("⚠ Buffer is full! Evicting a page before requesting a new one...")
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

<<<<<<< Updated upstream
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
            print(f"✅ Page {memory_page.position} has successfully been written to disk!")

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
=======
    # Evict Frame aka a Page Range with the Frame Data
    def evict_frame(self):
        with self.lock:
            if not self.frames:
                print("Eviction failed: No frames to evict")
                return None  # Nothing to evict

            frames_to_evict = self.get_least_needed_frames_first()
            
            if not frames_to_evict:
                print("Eviction failed: No suitable frame found")
                return None

            frame_to_evict = frames_to_evict[0]  # Pick the least-needed frame

            print(f"Evicting frame: {frame_to_evict.position} from {frame_to_evict.table_name}")

            # Write back if dirty
            if frame_to_evict.is_dirty:
                print(f"Writing dirty frame {frame_to_evict.position} before eviction")
                self.write_frame(frame_to_evict)

            # Remove from the buffer pool
            self.frames[frame_to_evict.table_name].remove(frame_to_evict)


>>>>>>> Stashed changes

    def on_close(self):
        for frame in self.frames:
            if frame.is_dirty:
                self.write_frame(frame)
            
            
            
            
    