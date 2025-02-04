from BTrees.OOBTree import OOBTree
import lstore.config as config
"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

# Some quick ideas...We know primary keys can be unique but if we choose to index other columns,
# there is no guarantee that the numbers there will be unique. For this reason, I believe that
# the Btree should store sets of RIDs as values. Since primary keys are unique, that's not needed but for other
# indexes it is.That way, you can iterate through each set if RIDs.
# For example, self.indices[1][10] = {20, 4, 19}. Here we are looking for records where the first column has a value of 10
# the records in question are stored in a set.

# The tree is made up of key, value pairs where the key is value in a column and value is an RID which
# can later be used by the page range.

class Index:

    def __init__(self, table):
      # One index for each table. All our empty initially.
      self.indices = [None] *  table.num_columns
      self.key = table.key
      self.indices[self.key] = OOBTree()



    def add(self, record):
      """
      A method for adding something to the index. This method would most likely be used during the
      insert query. From the record the RID can be extracted and then for the columns that contain
      indexes, the data from that column can also be added into the Btree.
      """
      rid = record[config.RID_COLUMN]
      for i, column_index in enumerate(self.indices):
        if column_index != None:
          # We have to do i + 4 because the first 4 columns are metadata columns. In other words, I am aligning
          column_index[record[i + 4]] = rid

    def delete(self, record):
      """
      This method would be used in the
      delete query. When a record is deleted you also call delete on the index. The RID is extracted
      and for each column that has an index, we search the index. For now this only works with unique primary keys
      """
      for i, column_index in enumerate(self.indices):
        if column_index != None:
          pk = record[i + 4]
          if pk in column_index:
            del column_index[record[i + 4]]
          else:
            raise Exception("They key was in the index")

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
      # Search down an index for a specific set of RIDs. If it's not found, then it doesn't exist
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
      # This one confuses me a bit too. Not the most efficient way but we could start with an empty
      # array then we iterate from begin to end and append each of the RIDs in the set to this array.
      # By the end of the function we should have an array with all RIDs that were within the specified range.
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
      # This function is going to take TIME. My idea is that we can use the primary key index to get all the RIDs
      # then we can use these RIDs to find the records in the table. From here we can read the column needed
      # and insert into the new tree along with RID. This is going to take time because we need to first get all the RIDs
      # then we have to read from the table. THEN we insert into the new tree.
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
      # just use del self.indices[column_number], though we shouldn't be allowed to delete the primary key column
        pass


