from lstore.conceptual_page import ConceptualPage
from lstore.page import Page
import lstore.config as config
from time import time
import os, json

class PageRange:

  def __init__(self, num_columns):
    self.meta_data_columns = config.NUM_META_COLUMNS
    self.regular_columns = num_columns
    self.total_columns = self.meta_data_columns + self.regular_columns
    self.base_pages = [ConceptualPage(num_columns)]
    self.base_pages_index = 0 # M3: TODO: Atomize
    self.base_pages_slot = 0 # M3: TODO: Atomize
    self.tail_pages = [ConceptualPage(num_columns)]
    self.tail_pages_index = 0 # M3: TODO: Atomize
    self.tail_pages_slot = 0 # M3: TODO: Atomize

  def has_base_page_capacity(self):
    """
    Determines whether you can continue writing records into the PageRange or not
    """
    # if you have less than 16 basepages you still have room. But if you have 16 pages, you have to
    # check if the last page has room
    return len(self.base_pages) < 16 or self.base_pages[-1].has_capacity()

  def __allocate_new_base_page(self):
    """
    Allocates a new basepage only if needed and changes the location pointers of where to write to
    """
    if len(self.base_pages) < 16 and not self.base_pages[-1].has_capacity():
      self.base_pages.append(ConceptualPage(self.regular_columns))
      self.base_pages_index += 1 # Move up the base pages we are writing into
      self.base_pages_slot = 0 # This should get reset from 4095 to 0

  def __allocate_new_tail_page(self):
    if not self.tail_pages[-1].has_capacity():
      self.tail_pages.append(ConceptualPage(self.regular_columns))
      self.tail_pages_index += 1  # Move up the base pages we are writing into
      self.tail_pages_slot = 0  # This should get reset from 4095 to 0

  def write_base_record(self, record):
    """
    Writes a new record into one of the base pages. Returns the location of the page within the
    page range. The entire record should be populated with the correct data, this includes metadata
    """
    # TODO: 2PL | Allocate up here just in case things changed
    if len(record) != self.total_columns:
      raise IndexError(f"Expected #{self.regular_columns}, you only gave #{len(record)} in PageRange")
    self.base_pages[self.base_pages_index].write_record(record)
    # Location is a tuple of base page index and slot, this should go in the page directory in the table along with PageRange it is in
    location = (self.base_pages_index, self.base_pages_slot)

    self.base_pages_slot += 1
    self.__allocate_new_base_page()

    return location

  def write_tail_record(self, record):
    if len(record) != self.total_columns:
      raise IndexError(f"Expected #{self.regular_columns}, you only gave #{len(record)} in PageRange")
    self.tail_pages[self.tail_pages_index].write_record(record)
    # Location is a tuple of base page index and slot, this should go in the page directory in the table along with PageRange it is in
    location = (self.tail_pages_index, self.tail_pages_slot)

    self.tail_pages_slot += 1
    self.__allocate_new_tail_page()

    return location

  def read_base_record(self, base_page_index, base_page_slot, projected_column_index):
    """
    This retrieves the regular data from base record. The record contains the metadata columns AND the regular data columns
    """
    meta_data = self.base_pages[base_page_index].read_metadata_at(base_page_slot)
    regular_data = self.base_pages[base_page_index].read_record_at(base_page_slot, projected_column_index)
    return meta_data + regular_data

  def read_tail_record(self, tail_page_index, tail_page_slot, projected_column_index):
    """
    Retrieves regular data from a tail record. Record contains metadata columns and regular data columns. Though if you
    just want metadata you can set the projected columns index to all 0s
    """
    meta_data = self.tail_pages[tail_page_index].read_metadata_at(tail_page_slot)
    regular_data = self.tail_pages[tail_page_index].read_record_at(tail_page_slot, projected_column_index)
    return meta_data + regular_data

  def update_base_record_column(self, base_page_index, base_page_slot, column, data):
    """
    A method should be used for actions like rewriting the schema encoding, rewriting the indirection column, setting the RID column
    to a delete value. This is only for base pages
    """
    self.base_pages[base_page_index].update_column(column, base_page_slot, data)

  def update_tail_record_column(self, tail_page_index, tail_page_slot, column, data):
    """
    A method should be used for actions like rewriting the schema encoding, rewriting the indirection column, setting the RID column
    to a delete value. This is only for tail pages
    """
    self.tail_pages[tail_page_index].update_column(column, tail_page_slot, data)

  def to_dict(self):
    data = {}
    data["meta_data_columns"] = self.meta_data_columns
    data["regular_columns"] = self.regular_columns
    data["base_pages_index"] = self.base_pages_index
    data["base_pages_slot"] = self.base_pages_slot
    data["tail_pages_index"] = self.tail_pages_index
    data["tail_pages_slot"] = self.tail_pages_slot
    # only tracks metadata, not the actual data. the conceptual pages have their own method of being stored
    # data["base_pages"] = [page.to_dict() for page in self.base_pages]
    # data["tail_pages"] = [page.to_dict() for page in self.tail_pages]
    return data

  @classmethod
  def from_dict(cls, data, path):
    new_page_range = cls(data["regular_columns"])
    new_page_range.meta_data_columns = data["meta_data_columns"]
    new_page_range.base_pages_index = data["base_pages_index"]
    new_page_range.base_pages_slot = data["base_pages_slot"]
    new_page_range.tail_pages_index = data["tail_pages_index"]
    new_page_range.tail_pages_slot = data["tail_pages_slot"]
    # only reads metadata, the actual data is read in the open_conceptual_pages() function
    # new_page_range.base_pages = [ConceptualPage.from_dict(page) for page in data["base_pages"]]
    # new_page_range.tail_pages = [ConceptualPage.from_dict(page) for page in data["tail_pages"]]

    # call function to get conceptual pages
    new_page_range.open_conceptual_pages(path)
    
    return new_page_range
  
  def save_contents(self, path):

    # save base pages
    for i in range(len(self.base_pages)):
      # save json of metadata
      bp_path = os.path.join(path, f"b{i}")
      with open(f"{bp_path}.json", "w+", encoding="utf-8") as file:
        json.dump(self.base_pages[i].to_dict(), file)
        
        # create a folder if it doesnt exist
        os.makedirs(bp_path, exist_ok=True)
      
      # save phys pages inside base page
      for j in range(len(self.base_pages[i].pages)):
        page = self.base_pages[i].pages[j]
        bp_col_path = os.path.join(bp_path, f"col{j}")
        # create a folder if it doesnt exist
        os.makedirs(bp_col_path, exist_ok=True)
        
        # write binary data to path
        for k in range(len(page)):
          bin_path = os.path.join(bp_col_path, f"{k}.bin")
          with open(bin_path, 'wb+') as data_file:
            data_file.write(page[k].data)
        
    # save tail pages
    for i in range(len(self.tail_pages)):
      # save json of metadata
      tp_path = os.path.join(path, f"t{i}")
      with open(f"{tp_path}.json", "w+", encoding="utf-8") as file:
        json.dump(self.tail_pages[i].to_dict(), file)
        
        # create a folder if it doesnt exist
        os.makedirs(tp_path, exist_ok=True)
      
      # save phys pages inside tail page
      for j in range(len(self.tail_pages[i].pages)):
        page = self.tail_pages[i].pages[j]
        tp_col_path = os.path.join(tp_path, f"col{j}")
        # create a folder if it doesnt exist
        os.makedirs(tp_col_path, exist_ok=True)
        
        # write binary data to disk
        for k in range(len(page)):
          bin_path = os.path.join(tp_col_path, f"{k}.bin")
          with open(bin_path, 'wb+') as data_file:
            data_file.write(page[k].data)

  def open_conceptual_pages(self, path):
    base_pairs = []
    tail_pairs = []

    for dir in os.listdir(path):
        if dir.startswith("b") and dir[1:].isdigit():  # Check if base
            num = int(dir[1:])  # get number
            folder_path = os.path.join(path, dir)
            json_path = os.path.join(path, f"b{num}.json")
            if os.path.isdir(folder_path) and os.path.isfile(json_path):
                base_pairs.append((folder_path, json_path))

        elif dir.startswith("t") and dir[1:].isdigit():  # Check if tail
            num = int(dir[1:])  # get number
            folder_path = os.path.join(path, dir)
            json_path = os.path.join(path, f"t{num}.json")
            if os.path.isdir(folder_path) and os.path.isfile(json_path):
                tail_pairs.append((folder_path, json_path))

    # order pairs numerically
    base_pairs.sort(key=lambda x: int(os.path.basename(x[0])[1:]))
    tail_pairs.sort(key=lambda x: int(os.path.basename(x[0])[1:]))
    
    # iterate through the base and tail pairs. send the json data and the path to the folder
    self.base_pages = [ConceptualPage.from_dict(path_json, path_folder) for path_folder, path_json in base_pairs]
    self.tail_pages = [ConceptualPage.from_dict(path_json, path_folder) for path_folder, path_json in tail_pairs]