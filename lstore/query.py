import time

from lstore.bufferpool import BufferPool, Frame
from lstore.page_range import PageRange
from lstore.table import Table
from lstore.record import Record
import lstore.config as config
from datetime import datetime

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table: Table):
        self.table: Table = table
        self.bufferpool: BufferPool = table.bufferpool
        pass

    def create_metadata(self, rid, indirection = 0, schema = 0):
      """
      Creates the metadata part of the record. Timestamp isn't included in the arguments because it is grabbed when this
      method is run.
      """
      record = [None] * 4

      record[config.RID_COLUMN] = rid
      record[config.INDIRECTION_COLUMN] = indirection
      record[config.TIMESTAMP_COLUMN] = int(time.time())
      record[config.SCHEMA_ENCODING_COLUMN] = schema

      return record

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        # After your all done, remove the primary key from the primary key index
        rids = self.table.index.locate(self.table.key, primary_key).copy()

        if not rids:
          return False

        for rid in rids:
          page_range_index, base_page_index, base_slot = self.table.page_directory[rid]

          frame: Frame = self.bufferpool.get_frame(self.table.name, page_range_index,
                                                   self.table.num_columns)
          frame.pin += 1
          page_range: PageRange = frame.page_range

          base_record = page_range.read_base_record(base_page_index, base_slot, [1] * self.table.num_columns)


          base_rid = rid
          page_range.update_base_record_column(base_page_index, base_slot, config.RID_COLUMN, 0)

          current_rid = base_record[config.INDIRECTION_COLUMN]
          while current_rid and current_rid != base_rid:
            # We know we don't have to get a new frame because any update to a base record in a page range goes to a
            # tail record in the same page range
            page_range_index, tail_index, tail_slot = self.table.page_directory[current_rid]

            tail_record = page_range.read_tail_record(tail_index, tail_slot,
                                                                                   [0] * self.table.num_columns)

            page_range.update_tail_record_column(tail_index, tail_slot, config.RID_COLUMN,
                                                                              0)
            current_rid = tail_record[config.INDIRECTION_COLUMN]

          self.table.index.delete(base_record)
          frame.is_dirty = True
          frame.pin -= 1
          self.table.page_directory[rid] = None

        return True


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def __bit_array_to_number(self,bit_array):
        return int("".join(map(str, bit_array)), 2)

    def __number_to_bit_array(self, num, bit_size):
            return [int(bit) for bit in f"{num:0{bit_size}b}"]

    def __build_record(self, record_data, search_key):
      time = datetime.fromtimestamp(float(record_data[config.TIMESTAMP_COLUMN]))
      schema_encoding = self.__number_to_bit_array(record_data[config.SCHEMA_ENCODING_COLUMN], self.table.num_columns)
      # I'm sorry this is a little long
      record = Record(record_data[config.INDIRECTION_COLUMN], record_data[config.RID_COLUMN], time, schema_encoding,
                      search_key, record_data[4:])
      return record

    def insert(self, *columns):
        if len(columns) != self.table.num_columns:
          return False

        pk_column = self.table.index.key

        if columns[pk_column] in self.table.index.indices[pk_column]:
          return False

        rid = self.table.new_rid()
        # TODO: Request Page logic (???) maybe link page range logic to bufferpool
        # bufferpool request range (self.table, index)
        # print(f"Insert on Page Range {self.table.name}, {self.table.page_ranges_index}, {self.table.num_columns}")
        frame: Frame = self.bufferpool.get_frame(self.table.name, self.table.page_ranges_index, self.table.num_columns)
        frame.pin += 1
        page_range: PageRange = frame.page_range

        frame.is_dirty = True
        # TODO: Decrement Pin Count

        # Create an array with the metadata columns, and then add in the regular data columns
        new_record = self.create_metadata(rid)
        for data in columns:
          new_record.append(data)
        # - write this record into the table
        index, slot = page_range.write_base_record(new_record)
        # - add the RID and location into the page directory
        self.table.page_directory[rid] = (self.table.page_ranges_index, index, slot)
        self.table.index.add(new_record)
      # - add the record in the index

        frame.pin -= 1

        # TODO: Question, why?
        self.table.add_new_page_range()

        return True

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist # This is nice to know
    """
    def select(self, search_key, search_key_index, projected_columns_index):
      # Select wants the latest version so this is select version 0
      return self.select_version(search_key, search_key_index, projected_columns_index, 0)

    def __select_base_records(self, search_key, search_key_index, projected_columns_index):
      records = []
      rids = self.table.index.locate(search_key_index, search_key)

      if rids is None:
        return []

      for rid in rids:
        page_range_index, base_page_index, slot = self.table.page_directory[rid]

        # Get the needed page range
        frame: Frame = self.bufferpool.get_frame(self.table.name, page_range_index, self.table.num_columns)
        frame.pin += 1
        page_range: PageRange = frame.page_range

        # We know we can read from a base record because the rid in a page directory points to a page record
        record_data = page_range.read_base_record(base_page_index, slot, projected_columns_index)
        record = self.__build_record(record_data, search_key)
        records.append(record)

        frame.pin -= 1

      return records
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
      records = self.__select_base_records(search_key, search_key_index, projected_columns_index)
      version_records = [] # To store the wanted versions of the records

      for base_record in records:
        if base_record.indirection == 0: # This means it is a base record so there is no other versions
          version_records.append(base_record)
        else: # There are some tail records
          # Version 0 is the latest version
          version_num = 0
          current_rid = base_record.indirection
          page_range_index, tail_index, tail_slot = self.table.page_directory[current_rid]

          frame: Frame = self.bufferpool.get_frame(self.table.name, page_range_index,
                                                   self.table.num_columns)
          frame.pin += 1
          page_range: PageRange = frame.page_range
          return_base_record = False
          # Get the tail record
          tail_record = page_range.read_tail_record(tail_index, tail_slot, projected_columns_index)
          # if the indirection for the tail record is the base record rid we hit the end, so just return the base record
          # else combine the data from the latest tail with columns that haven't been updated in base.
          while version_num > relative_version:
            if tail_record[config.INDIRECTION_COLUMN] == base_record.rid:
              return_base_record = True
              break
            current_rid = tail_record[config.INDIRECTION_COLUMN]
            page_range_index, tail_index, tail_slot = self.table.page_directory[current_rid]
            tail_record = page_range.read_tail_record(tail_index, tail_slot,
                                                                                    projected_columns_index)
            version_num -= 1

          frame.pin -= 1

          # relative_version < version_num means that we ran out of versions to traverse
          if return_base_record or relative_version < version_num:
            version_records.append(base_record)
          else:
            for i, data in enumerate(self.__number_to_bit_array(tail_record[config.SCHEMA_ENCODING_COLUMN], self.table.num_columns)):
              # Only add in the updated data
              if data == 1:
                base_record.columns[i] = tail_record[4 + i]

          version_records.append(base_record)




      return version_records



    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
      pk_column = self.table.key
      # You shouldn't be allowed to change the primary key
      if columns[pk_column] is not None and primary_key != columns[pk_column]:
        return False

      rids = self.table.index.locate(self.table.key, primary_key)

      # Now that we have the RIDS, lets remove the old version of the records from our index, but this will
      # be done after we are done updating
      old_records = self.select(primary_key, self.table.index.key, [1] * self.table.num_columns)

      if rids is None:
        return False

      for rid in rids:
        page_range_index, base_page_index, base_slot = self.table.page_directory[rid]

        frame: Frame = self.bufferpool.get_frame(self.table.name, page_range_index,
                                                 self.table.num_columns)
        frame.pin += 1
        page_range: PageRange = frame.page_range

        base_record = page_range.read_base_record(base_page_index, base_slot, [0] * self.table.num_columns)

        if base_record[config.INDIRECTION_COLUMN] == 0:
          tail_rid = self.table.new_rid()
          updated_schema_encoding = [0] * self.table.num_columns

          # Set ones for the changed things
          for i, data in enumerate(columns):
            if data is not None:
              updated_schema_encoding[i] = 1
          schema_encoding_num = self.__bit_array_to_number(updated_schema_encoding)

          record_data = self.create_metadata(tail_rid, base_record[config.RID_COLUMN], schema_encoding_num)
          for data in columns:
            if data is not None:
              record_data.append(data)
            else:
              record_data.append(0)

          tail_index, tail_slot = page_range.write_tail_record(record_data)
          self.table.page_directory[tail_rid] = (page_range_index, tail_index, tail_slot)
          page_range.update_base_record_column(base_page_index, base_slot, config.INDIRECTION_COLUMN, tail_rid)
          page_range.update_base_record_column(base_page_index, base_slot, config.SCHEMA_ENCODING_COLUMN, schema_encoding_num)
        else:
          # The base record has a tail record. This is the cumulative implementation.
          # v Contains all the columns that have been updated so far v
          base_record_updated_schema_encoding = self.__number_to_bit_array(base_record[config.SCHEMA_ENCODING_COLUMN], self.table.num_columns)
          latest_tail_rid = base_record[config.INDIRECTION_COLUMN]
          page_range_index, latest_tail_index, latest_tail_slot = self.table.page_directory[latest_tail_rid]

          latest_tail_record = page_range.read_tail_record(latest_tail_index, latest_tail_slot, base_record_updated_schema_encoding)
          new_tail_rid = self.table.new_rid()

          for i, data in enumerate(columns):
            if data is not None: # Combining schema encodings
              base_record_updated_schema_encoding[i] = 1
          tail_schema_encoding_num = self.__bit_array_to_number(base_record_updated_schema_encoding)

          new_tail_record_data = self.create_metadata(new_tail_rid, latest_tail_rid, tail_schema_encoding_num)
          for i, data in enumerate(columns):
            if data is not None:
              new_tail_record_data.append(data)
            elif latest_tail_record[i + 4]: # If there's data actually there
              new_tail_record_data.append(latest_tail_record[i + 4]) # Offset for metadata columns
            else:
              new_tail_record_data.append(0)

          new_tail_index, new_tail_slot = page_range.write_tail_record(new_tail_record_data)
          self.table.page_directory[new_tail_rid] = (page_range_index, new_tail_index, new_tail_slot)
          page_range.update_base_record_column(base_page_index, base_slot, config.INDIRECTION_COLUMN, new_tail_rid)
          page_range.update_base_record_column(base_page_index, base_slot, config.SCHEMA_ENCODING_COLUMN,
                                               tail_schema_encoding_num)

        frame.is_dirty = True
        frame.pin -= 1


      new_records = self.select(primary_key, self.table.index.key, [1] * self.table.num_columns)
      # Remove the old records from the index and...
      for record in old_records:
        full_record = [record.indirection, record.rid, record.timestamp, record.schema_encoding]
        full_record = full_record + record.columns
        self.table.index.delete(full_record)

      # add in the new records
      for record in new_records:
        full_record = [record.indirection, record.rid, record.timestamp, record.schema_encoding]
        full_record = full_record + record.columns
        self.table.index.add(full_record)


      return True

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
      total_sum = 0
      rids = []
      for primary_key in range(start_range, end_range + 1):
        record = self.select(primary_key, self.table.key,
                                     [1 if i == aggregate_column_index else 0 for i in range(self.table.num_columns)])
        if record and record is not False:
          value_to_sum = record[0].columns[aggregate_column_index]
          if value_to_sum is None:
            value_to_sum = 0
          total_sum += value_to_sum
          rids.append(record)

      if not rids :
        return False

      return total_sum


    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
      total_sum = 0
      rids = []
      for primary_key in range(start_range, end_range + 1):
        record = self.select_version(primary_key, self.table.key,
                             [1 if i == aggregate_column_index else 0 for i in range(self.table.num_columns)], relative_version)
        if record and record is not False:
          value_to_sum = record[0].columns[aggregate_column_index]
          if value_to_sum is None:
            value_to_sum = 0
          total_sum += value_to_sum
          rids.append(record)

      if not rids:
        return False

      return total_sum


    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False