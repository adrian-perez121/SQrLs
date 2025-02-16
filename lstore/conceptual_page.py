from lstore.page import Page  # Assuming your Page class is defined in page.py
import lstore.config as config
import os
import shutil

class ConceptualPage:
  def __init__(self, num_columns):
    """
    Initialize a base page with metadata and column pages.
    :param num_columns: Number of data columns (excluding metadata columns).
    """
    self.num_records = 0  # Tracks the total number of records, also treated as next available slot
    self.metadata_columns = 4  # Fixed number of metadata columns
    self.regular_columns = num_columns
    self.total_columns = self.metadata_columns + self.regular_columns
    # For the sake of consistency, the first four columns should be the metadata columns
    # Since everything is lazy, we start off with one row of column pages and expand until we reach 16
    # BasePages holds 4096 records = 512 records * 8 physical pages PER COLUMN
    self.pages = [[Page()] for _ in range(self.total_columns)]

  def __allocate_new_physical_pages(self):
    # First check is so we don't go over 8 ConceptualPages
    # The second check makes sure the next_slot (num_records) can't be stored in a physical page we already have
    physical_page_level = self.num_records // 512
    if physical_page_level <= 7 and physical_page_level >= len(self.pages[0]):
      for column in self.pages:
        column.append(Page())

  def has_capacity(self):
    """
    Checks if the base page has enough space for a new record.
    """
    return self.num_records < 4096


  def read_record_at(self, slot, projected_columns_index):
    """
    Returns an array based off of the projected columns index. This does not return any metadata. Just the record
    """
    if slot < 0 or slot >= self.num_records:
      raise IndexError("No record to read in that slot")

    if len(projected_columns_index) != self.regular_columns:
      raise Exception(f"In ConceptualPage, expected projected columns index of size #{self.regular_columns}, not #{len(projected_columns_index)}")

    # Project columns index is an array of 1s and 0s. 1 is a column you want
    # 0 is a column you don't want
    physical_page_level = slot // 512
    physical_page_slot = slot % 512
    record = []
    # Ignore the metadata columns
    for i in range(self.metadata_columns, self.total_columns):
      # If true we want this column, we subtract because the project_columns array starts at 0
      if projected_columns_index[i - self.metadata_columns]:
        record.append(self.pages[i][physical_page_level].read(physical_page_slot))
      else:
        record.append(None)

    return record

  def read_metadata_at(self, slot):
    """
    Specifically only for reading the metadata of a record. Useful if you need to update something
    like the indirection column or schema encoding.
    """
    physical_page_level = slot // 512
    physical_page_slot = slot % 512
    record = []
    for i in range(self.metadata_columns):
      # the first 4 columns are the metadata columns
      record.append(self.pages[i][physical_page_level].read(physical_page_slot))

    return record

  def write_record(self, record):
    """
      Adding a new record in the Conceptual Page. This can be for base pages or tail pages. So please be sure
      of which pages you are writing into. This should take in data for the metadata columns as well
    """
    if not self.has_capacity():
      raise IndexError("Conceptual Page is full")

    if len(record) != self.total_columns:
      raise Exception("Not enough columns")

    new_slot = self.num_records
    physical_page_level = new_slot // 512
    physical_page_slot = new_slot % 512

    for column_num, data in enumerate(record):
      self.pages[column_num][physical_page_level].write(data, physical_page_slot)

    self.num_records += 1
    self.__allocate_new_physical_pages() # In case you fill the physical page up

  def update_column(self, column ,slot, new_indirection):
    """
    Should primarily be used for updating the indirection and schema encoding columns.
    Use carefully because we aren't supposed to update data in place for most cases
    """
    if slot < 0 or slot > self.num_records:
      raise IndexError("Invalid slot trying to be accessed")

    physical_page_level = slot // 512
    physical_page_slot = slot % 512
    self.pages[column][physical_page_level].write(new_indirection, physical_page_slot)

  def dump_file(self, dir_name):
    # Write over it if the directory exists
    if os.path.isdir(dir_name):
      shutil.rmtree(dir_name)

    os.mkdir(dir_name)

    # Some meta data stuff {
    with open(f'./{dir_name}/regular_columns', 'wb') as file:
      file.write(self.regular_columns.to_bytes(8, byteorder='big'))

    with open(f'./{dir_name}/num_records', 'wb') as file:
      file.write(self.num_records.to_bytes(8, byteorder='big'))
    # }

    for i, column in enumerate(self.pages):
      os.mkdir(f'./{dir_name}/' + str(i))

      for j, physical_page in enumerate(column):
        with open(f'./{dir_name}/' + str(i) + "/" +str(j), 'wb') as file:
          file.write(physical_page.data)


  @classmethod
  def load_file(cls, dir_name):
    num_columns = None
    num_records = None
    with open(f'./{dir_name}/regular_columns', 'rb') as file:
      num_columns = int.from_bytes(file.read(), byteorder='big')

    with open(f'./{dir_name}/num_records', 'rb') as file:
      num_records = int.from_bytes(file.read(), byteorder='big')


    conceptual_page = cls(num_columns)
    # The last two things were meta data for conceptual page
    pages_names = sorted(os.listdir(f"./{dir_name}"))[:-2]
    pages = []

    for column in pages_names:
      pages.append([])
      column_num = int(column)
      physical_pages = sorted(os.listdir(f"./{dir_name}/{column}"))

      # add the physical pages into each column
      for physical_page in physical_pages:
        page = Page()
        with open(f"./{dir_name}/{column}/{physical_page}", 'rb') as file:
         page.data = file.read()

        pages[column_num].append(page)

      full_pages = num_records // 512

      # Change physical page metadata to be correct
      for i in range(full_pages):
        pages[column_num][i].num_records = 512

      if num_records == 4096: # Edge case
        pages[column_num][-1].num_records = 512
      else: # The last page is the one that was last added to
        pages[column_num][-1].num_records = num_records % 512

    # Adjust conceptual page meta and return
    conceptual_page.pages = pages
    conceptual_page.num_records = num_records

    return conceptual_page






