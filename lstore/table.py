from lstore.atomic import Atomic
from lstore.index import Index
from lstore.page_range import PageRange
from time import time

# 4 Meta Data Columns {
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
# }

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_ranges = [PageRange(num_columns)]
        self.page_ranges_index = 0
        self.page_directory = {}
        self.index = Index(self)
        self.rid = Atomic(1)
        pass

    def new_rid(self):
      def get_current_rid_and_increment(self):
        tmp = self.rid
        return self.rid + 1
      self.rid.modify
      

    def add_new_page_range(self):
      # Check if the last added page range has room for more baserecords
      # If not add a new one and move the index up
      if not self.page_ranges[-1].has_base_page_capacity():
        self.page_ranges.append(PageRange(self.num_columns))
        self.page_ranges_index += 1

    def __merge(self):
        print("merge is happening")
        pass


