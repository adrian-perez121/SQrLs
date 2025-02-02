from lstore.conceptual_page import ConceptualPage
import lstore.config as config
from time import time

class PageRange:

  def __init__(self, num_columns):
    self.meta_data_columns = 4
    self.regular_columns = num_columns
    self.total_columns = self.meta_data_columns + self.regular_columns
    self.base_pages = [ConceptualPage(num_columns)]
    self.base_pages_index = 0
    self.base_pages_slot = 0
    self.tail_pages = [ConceptualPage(num_columns)]
    self.tail_pages_index = 0
    self.tail_pages_slot = 0
    self.tail_page_directory = {}

  def has_base_page_capacity(self):
    """
    Determines whether you can continue writing records into the PageRange or not
    """
    # if you have less than 16 basepages you still have room. But if you have 16 pages, you have to
    # check if the last page has room
    return len(self.base_pages) < 16 or self.base_pages[-1].has_capacity()

  def __alocate_new_base_page(self):
    """
    Allocates a new basepage only if needed and changes the location pointers of where to write to
    """
    if len(self.base_pages) < 16 and not self.base_pages[-1].has_capacity():
      self.base_pages.append(ConceptualPage(self.regular_columns))
      self.base_pages_index += 1 # Move up the base pages we are writing into
      self.base_pages_slot = 0 # This should get reset from 4095 to 0


  def write_base_record(self, record):
    """
    Writes a new record into one of the base pages. Returns the location of the page within the
    page range. The entire record should be populated with the correct data, this includes metadata
    """
    if len(record) != self.total_columns:
      raise IndexError(f"Expected #{self.regular_columns}, you only gave #{len(record)} in PageRange")
    self.base_pages[self.base_pages_index].write_record(record)
    # Location is a tuple of base page index and slot, this should go in the page directory in the table along with PageRange it is in
    location = (self.base_pages_index, self.base_pages_slot)

    self.base_pages_slot += 1
    self.__alocate_new_base_page()

    return location

  def read_base_record(self, base_page_index, base_page_slot, project_column_index):
    """
    This retrieves the regular data from base record. The record contains the metadata columns AND the regular data columns
    """
    meta_data = self.base_pages[base_page_index].read_metadata_at(base_page_slot)
    regular_data = self.base_pages[base_page_index].read_record_at(base_page_slot, project_column_index)
    return meta_data + regular_data






