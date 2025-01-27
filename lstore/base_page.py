from lstore.page import Page  # Assuming your Page class is defined in page.py
from collections import deque


class BasePage:
  def __init__(self, num_columns):
    """
    Initialize a base page with metadata and column pages.
    :param num_columns: Number of data columns (excluding metadata columns).
    """
    self.num_records = 0  # Tracks the total number of records
    self.metadata_columns = 4  # Fixed number of metadata columns
    self.regular_columns = num_columns
    self.total_columns = self.metadata_columns + self.regular_columns

    # A queue from 0 to 511. We keep track of the slot we write to through this data structure, this way, everything aligns (hopefully)
    self.next_slot = deque(range(0, 512))
    # A page for each column + meta_data columns
    # For the sake of consistency, the last four columns should be the metadata columns
    self.pages = [Page() for _ in range(self.total_columns)]


  def has_capacity(self):
    """
    Checks if the base page has enough space for a new record.
    """

    # When there are no more available slots, the page won't have any more capacity
    return len(self.next_slot) > 0


  def get_record(self, slot, projected_columns_index):
    """
    A method for returning the record in the base page. Method only reads columns that
    are specified in the projected_columns_index array.
    """

    # Project columns index is an array of 1s and 0s. 1 is a column you want
    # 0 is a column you don't want
    record = []
    column_pages = self.pages[: self.regular_columns] # Ignore the meta data
    for index, page in enumerate(column_pages):
      if projected_columns_index[index]:
        record.append(page.read(slot))
      else:
        record.append(None)

    return record


  def new_record(self, record):
    """
    Adding a new record to the base page. You are only ever able write new records and delete
    existing records. You never update existing records.
    :param record: a list of what's being added into the columns
    """
    if not self.has_capacity():
      raise "Page is full"

    if len(record) > self.regular_columns:
      raise "Not enough columns"

    # NOTE: Nothing is being inserted into the metadata columns here

    slot = self.next_slot.popleft()
    for i, data in enumerate(record):
      self.pages[i].write(data, slot)
