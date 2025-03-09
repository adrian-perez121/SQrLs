import time
from binascii import b2a_hex

from lstore.bufferpool import BufferPool, Frame
from lstore.page_range import PageRange
from lstore.table import Table
from lstore.record import Record
import lstore.config as config
from datetime import datetime
from lstore.lock_manage import LockManage
from BTrees.OOBTree import OOBTree


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table: Table):
        self.table: Table = table
        self.lock_manage = table.lock_manage  # use table's lock_manage
        self.bufferpool: BufferPool = table.bufferpool

    def create_metadata(self, rid, indirection = 0, schema = 0):
      """
      Creates the metadata part of the record. Timestamp isn't included in the arguments because it is grabbed when this
      method is run.
      """
      record = [0] * config.NUM_META_COLUMNS

      record[config.RID_COLUMN] = rid
      record[config.INDIRECTION_COLUMN] = indirection
      record[config.TIMESTAMP_COLUMN] = int(time.time())
      record[config.SCHEMA_ENCODING_COLUMN] = schema

      return record

    def __update_schema_encoding(self, current_schema_encoding, updated_columns):
      for i, data in enumerate(updated_columns):
        if data is not None:
          current_schema_encoding[i] = 1
      return current_schema_encoding

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        """ Deletes a record, acquiring an exclusive lock. """
        
        # locate all RIDs for the primary key
        rids = self.table.index.locate(self.table.key, primary_key)
        
        if not rids:
            return False
        
        for rid in rids:
            # acquire exclusive lock for delete
            if not self.lock_manage.acquire_record_lock(rid, id(self), "E"):
                raise Exception(f"Transaction {id(self)} failed to acquire lock for DELETE {rid}")

        try:
            for rid in rids:
                page_range_index, base_page_index, base_slot = self.table.page_directory[rid]

                frame: Frame = self.bufferpool.get_frame(self.table.name, page_range_index, self.table.num_columns)
                frame.pin += 1
                page_range: PageRange = frame.page_range

                # read base record for reference
                base_record = page_range.read_base_record(base_page_index, base_slot, [1] * self.table.num_columns)

                # set RID to 0 to indicate deletion
                page_range.update_base_record_column(base_page_index, base_slot, config.RID_COLUMN, 0)

                # remove from ndex
                self.table.index.delete(base_record)

                frame.is_dirty = True
                frame.pin -= 1

                # remove from page directory
                del self.table.page_directory[rid]

            return True

        except Exception as e:
            print(f"Delete failed: {e}")
            for rid in rids:
                self.lock_manage.release_record_lock(rid, id(self))  # release
            return False


    def undo_delete(self, base_rid):
        page_range_index, base_page_index, base_slot = self.table.page_directory[base_rid]

        frame: Frame = self.bufferpool.get_frame(self.table.name, page_range_index,
                                                 self.table.num_columns)
        frame.pin += 1
        page_range: PageRange = frame.page_range

        base_record = page_range.read_base_record(base_page_index, base_slot, [1] * self.table.num_columns)
        base_record[config.RID_COLUMN] = base_rid

        page_range.update_base_record_column(base_page_index, base_slot, config.RID_COLUMN, base_rid)

        self.table.index.add(base_record)
        frame.is_dirty = True
        frame.pin -= 1

        return



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
      record_data[config.TIMESTAMP_COLUMN] = time
      record_data[config.SCHEMA_ENCODING_COLUMN] = schema_encoding

      record = Record(record_data, search_key)
      return record

    def insert(self, *columns):
      if len(columns) != self.table.num_columns:
          print(f"Insert failed: column length mismatch ({len(columns)} != {self.table.num_columns})")
          return False

      pk_column = self.table.index.key
      existing_keys = self.table.index.indices.get(pk_column, {})

      if not isinstance(existing_keys, (dict, OOBTree)):
          existing_keys = OOBTree()

      if columns[pk_column] in existing_keys:
          print(f"Insert failed: Duplicate primary key {columns[pk_column]}")
          return False  # prevents multiple primary key insertions
      
      rid = columns[self.table.index.key]  # use primary key as RID

      # lock for E insert
      if not self.lock_manage.acquire_record_lock(rid, id(self), "E"):
          print(f"Insert failed: Could not acquire lock for RID {rid}")
          return False
      
      try:
          frame: Frame = self.bufferpool.get_frame(self.table.name, self.table.page_ranges_index, self.table.num_columns)
          frame.pin += 1
          page_range: PageRange = frame.page_range

          new_record = [0] * config.NUM_META_COLUMNS + list(columns)
          index, slot = page_range.write_base_record(new_record)

          self.table.page_directory[rid] = (self.table.page_ranges_index, index, slot)
          print(f"Inserted: RID={rid}, page_range={self.table.page_ranges_index}, index={index}, slot={slot}")

          if not isinstance(self.table.index.indices.get(pk_column), OOBTree):
              self.table.index.indices[pk_column] = OOBTree()

          self.table.index.add(new_record)

          frame.is_dirty = True
          frame.pin -= 1
          self.table.add_new_page_range()
          return True
      
      except Exception as e:
          print(f"Insert failed: {e}")
          self.lock_manage.release_record_lock(rid, id(self))  # release lock
          return False


    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist # This is nice to know
    """
    def select(self, rid, search_key_index, projected_columns_index):
      if rid not in self.table.page_directory:
          print(f"Select failed: RID: {rid} not found in the page_directory. Available RIDs: {list(self.table.page_directory.keys())}")
          return []

      page_range_index, base_page_index, slot = self.table.page_directory[rid]

      frame: Frame = self.bufferpool.get_frame(self.table.name, page_range_index, self.table.num_columns)
      frame.pin += 1
      page_range: PageRange = frame.page_range

      record_data = page_range.read_base_record(base_page_index, slot, projected_columns_index)
      frame.pin -= 1

      if not record_data:
          print(f"Select failed: No data found for this RID: {rid}")
          return []

      return [self.__build_record(record_data, rid)]


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
            tail_record = page_range.read_tail_record(tail_index, tail_slot, projected_columns_index)
            version_num -= 1

          frame.pin -= 1

          # relative_version < version_num means that we ran out of versions to traverse
          if return_base_record or relative_version < version_num:
            version_records.append(base_record)
          else:
            for i, data in enumerate(self.__number_to_bit_array(tail_record[config.SCHEMA_ENCODING_COLUMN], self.table.num_columns)):
              # Only add in the updated data
              if data == 1:
                base_record.columns[i] = tail_record[config.NUM_META_COLUMNS + i]

          version_records.append(base_record)




      return version_records


    def undo_latest_update(self, base_rid):
      # Note: This should only run when the base_record has an update
      page_range_index, bp_index, bp_slot =  self.table.page_directory[base_rid]

      frame: Frame = self.bufferpool.get_frame(self.table.name, page_range_index,
                                               self.table.num_columns)
      frame.pin += 1
      page_range: PageRange = frame.page_range

      base_record = page_range.read_base_record(bp_index, bp_slot, [1] * self.table.num_columns)
      latest_tail_rid = base_record[config.INDIRECTION_COLUMN]
      page_range_index, tp_index, tp_slot = self.table.page_directory[latest_tail_rid]
      latest_tail_record = page_range.read_tail_record(tp_index, tp_slot, [1] * self.table.num_columns)

      latest_version = base_record

      for i, data in enumerate(
        self.__number_to_bit_array(latest_tail_record[config.SCHEMA_ENCODING_COLUMN], self.table.num_columns)):
        # Only add in the updated data
        if data == 1:
          latest_version[config.NUM_META_COLUMNS + i] = latest_tail_record[config.NUM_META_COLUMNS + i]

      self.table.index.delete(latest_version) # Maintaining the index

      behind_latest_rid = latest_tail_record[config.INDIRECTION_COLUMN]

      if behind_latest_rid == base_rid: # This will change the indirection for the base_record back to 0
        behind_latest_rid = 0
        self.table.index.add(base_record) # Maintaining the index
      else:
        page_range_index, new_latest_tail, new_latest_slot = self.table.page_directory[behind_latest_rid]
        new_latest_tail = page_range.read_tail_record(new_latest_tail, new_latest_slot, [1] * self.table.num_columns)
        updated_record = base_record
        for i, data in enumerate(
          self.__number_to_bit_array(new_latest_tail[config.SCHEMA_ENCODING_COLUMN], self.table.num_columns)):
          # Only add in the updated data
          if data == 1:
            updated_record[config.NUM_META_COLUMNS + i] = new_latest_tail[config.NUM_META_COLUMNS + i]
        self.table.index.add(updated_record) # Maintaining the index

      # Pretty much skipping over the last update
      page_range.update_base_record_column(bp_index, bp_slot, config.INDIRECTION_COLUMN, behind_latest_rid)

      return
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

      if not rids:
        return False

      try:
        # track old records for index update
        old_records = self.select(primary_key, self.table.index.key, [1] * self.table.num_columns)

        for rid in rids:
            page_range_index, base_page_index, base_slot = self.table.page_directory[rid]

            frame: Frame = self.bufferpool.get_frame(self.table.name, page_range_index, self.table.num_columns)
            frame.pin += 1
            page_range: PageRange = frame.page_range

            # read base record
            base_record = page_range.read_base_record(base_page_index, base_slot, [0] * self.table.num_columns)

            # if first update or an additional update
            tail_rid = self.table.new_rid()
            prev_rid = base_record[config.INDIRECTION_COLUMN] or rid

            # schema
            schema_encoding = self.__update_schema_encoding([0] * self.table.num_columns, columns)
            schema_encoding_num = self.__bit_array_to_number(schema_encoding)

            # new tail record
            tail_record = self.create_metadata(tail_rid, prev_rid, schema_encoding_num)

            # potential fix to ensure the values are copied right
            for i, data in enumerate(columns):
                if data is not None:
                    tail_record.append(data)  # update val
                else:
                    tail_record.append(base_record[i + config.NUM_META_COLUMNS])  # keep old data
            # write tail record
            tail_index, tail_slot = page_range.write_tail_record(tail_record)
            self.table.page_directory[tail_rid] = (page_range_index, tail_index, tail_slot)

            # update base record indirection & schema encoding
            page_range.update_base_record_column(base_page_index, base_slot, config.INDIRECTION_COLUMN, tail_rid)
            page_range.update_base_record_column(base_page_index, base_slot, config.SCHEMA_ENCODING_COLUMN, schema_encoding_num)

            frame.is_dirty = True
            frame.pin -= 1

        # remove old index entries & add new ones
        for record in old_records:
            self.table.index.delete(record.entire_record)

        new_records = self.select(primary_key, self.table.index.key, [1] * self.table.num_columns)
        for record in new_records:
            self.table.index.add(record.entire_record)
        print(f"Updating record: {base_record} with new tail record: {tail_record}")

        return True

      except Exception as e:
          print(f"Update failed: {e}")
          for rid in rids:
              self.lock_manage.release_record_lock(rid, id(self))  # release lock on failure
          return False

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

    def undo_insert(self, rid, primary_key, isBaseRecord=bool):
      page_range_index, page_index, page_slot = self.table.page_directory[rid]
      frame: Frame = self.bufferpool.get_frame(self.table.name, page_range_index, self.table.num_columns)
      frame.pin += 1
      page_range: PageRange = frame.page_range

      old_records = None
      if isBaseRecord:
        base_record = page_range.read_base_record(page_index, page_slot, [1] * self.table.num_columns)
        page_range.update_base_record_column(page_index, page_slot, config.RID_COLUMN, 0)
        self.table.index.delete(base_record)
      else:
        old_records = self.select(primary_key, self.table.index.key, [1] * self.table.num_columns)
        page_range.update_tail_record_column(page_index, page_slot, config.RID_COLUMN, 0)

      frame.is_dirty = True
      frame.pin -= 1
      del self.table.page_directory[rid]

      if not isBaseRecord:
        new_records = self.select(primary_key, self.table.index.key, [1] * self.table.num_columns)
        # Remove the old records from the index and...
        for record in old_records:
          self.table.index.delete(record.entire_record)

        # add in the new records
        for record in new_records:
          self.table.index.add(record.entire_record)


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
