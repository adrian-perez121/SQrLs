
import os
import time
import json
from typing import Optional

from lstore.conceptual_page import ConceptualPage
from lstore.page_range import PageRange

class Frame:
    def __init__(self, table_name: str, position: int, page_range: PageRange, from_disk: bool = False):
        self.is_dirty: bool = not from_disk # Pages not read from disk should be dirty so that they are written
        self.request_count: int = 0
        self.last_accessed = time.time()
        self.table_name: str = table_name
        self.position: int = position
        self.pin = 0
        
        self.page_range: PageRange = page_range
        
    def to_dict(self):
        # TODO
        pass
    
def frame_from_dict(dict, table_name: str, position: int, num_columns: int, from_disk: bool = True) -> Frame:
        # TODO
        # New Frame with correct position, table_name, and usually from disk should be true if using this function
        # Use num of columns if necessary, but you could probably store it in the dict somewhere to access on read
        pass


class BufferPool:
    
    # Memory Page
        # self.request_count
        # self.last_access
    
    def __init__(self, path):
        self.frame_request_count = 0 # For Most Used
        self.frames: dict[str, list[Frame]] = {}
        self.capacity = 16 # M3: Do we need more frames?
        self.path = path
        self.next_file_name = 0
        self.files = {file: os.path.join(path, file) for file in os.listdir(path)}
        pass
    
    def get_frame(self, table_name: str, page_range_index: int, num_columns: int):
        # TODO: HAS CAPACITY if not evict
        # TODO: Diego: Very incomplete, list needs to expand to right index and table key needs to be put in at some point possibly here along with an empty expandable list
        self.frames.setdefault(table_name, [])
        if table_name in self.frames:
            frames: list[Frame] = self.frames[table_name]
            append_count = max(0, page_range_index - len(frames) + 1)
            frames.extend([None] * append_count)
            frame: Frame = frames[page_range_index]
            if not frame:
                frame = self.read_frame(table_name, page_range_index, num_columns)
                frames[page_range_index] = frame
            frame.request_count += 1
            return frame
        else:
            # 
            # TODO OOPS
            pass
    
    def has_capacity(self):
        return len(self.memory_pages) <= self.capacity
    
    def read_frame(self, table_name: str, page_range_index: int, num_columns: int):
        # TODO Look into possible issues with this way of reading
        read_path = os.path.join(self.path, "tables", table_name, f"{page_range_index}.json")
        if os.path.exists(read_path):
            try:
                with open(read_path, "rb") as file:
                    self.frames[table_name][page_range_index] = frame_from_dict(json.load(file), position = page_range_index, table_name = table_name, num_columns = num_columns)
            except Exception as e:
                print(f"Exception raised while reading frame from disk: {e}")
        else:
            Frame(table_name, page_range_index, PageRange(num_columns))
    
    def write_frame(self, frame: Frame):
        # TODO Look into possible issues with this way of writing
        write_path = os.path.join(self.path, "tables", frame.table_name)
        os.makedirs(write_path, exist_ok = True)
        if os.path.exists(write_path):
            frame_path = os.path.join(write_path, f"{frame.position}.json")
            if os.path.exists(frame_path):
                # print("Overwrite Log")
                pass
            try:
                with open(frame_path, "w", encoding = "utf-8") as file:
                    json.dump(frame.to_dict(), file)
            except Exception as e:
                print(f"Exception raised while writing frame to disk: {e}")
        else:
            print("Somehow the write path didn't exist after making it.")
            pass
    
    def is_pinned(self):
        # determine whether or not the page is pinned (currently being used)
        # return bool 
        return self.memory_pages.pins == 1 # currently in use
        #pass
    
    def get_least_needed_frame(self) -> Optional[Frame]:
        all_frames: list[Frame] = []
        for key in self.frames.keys:
            all_frames.extend(self.frames[key])
        sorted_frames = sorted(all_frames, key = Frame.request_count) # Frames sorted from least requested to most requested
        stop_index = 0
        least_requests = 0
        for i, frame in enumerate(sorted_frames):
            if i == 0:
                least_requests = frame.request_count
            if frame.request_count > least_requests:
                stop_index = i
                break
        sorted_frames = sorted(sorted_frames[0:stop_index], key = Frame.last_accessed) # Least requested pages sorted from oldest access to most recent access
        return sorted_frames[0] if sorted_frames else None
            
    # Evict Frame aka a Page Range with the Frame Data
    def evict_frame(self):
        # If Frame Dirty, write to Disk
        if self.frames and not self.has_capacity():
            frame = self.get_least_needed_frame()
            if frame.is_dirty:
                
                if frame.pins: # M3: TODO might not work if pins is atomic
                    # M3: Try to evict something else
                    # TODO
                    pass
                else:
                    if frame.table_name in self.frames:
                        self.write_frame(frame)
                        self.frames[frame.table_name][frame.position] = None
        pass
    
    def on_close(self):
        for frame in self.frames:
            if frame.is_dirty:
                self.write_frame(frame)
            
            
            
            
    