import random
import sys

from lstore.table import Table
import lstore.config as config

sys.path.append('../lstore')
import unittest


class MyTestCase(unittest.TestCase):
  def __record_builder(self, rid, *columns):
    record = [None] * 4

    # We don't care about these columns right now
    record[config.RID_COLUMN] = rid
    record[config.INDIRECTION_COLUMN] = 0
    record[config.TIMESTAMP_COLUMN] = 0
    record[config.SCHEMA_ENCODING_COLUMN] = 0

    for data in columns:
      record.append(data)
    return record

  def test_add_and_delete_and_locate(self):
    # For now, I am only going to test index
    table = Table("test_table", 3, 0)
    index = table.index

    used_rids = set()
    used_pks = set()
    # An array of tuples to practice deleting
    records = []
    for i in range(20):
      # Generating a random set of unique RIDs and PKs {
      rid = random.randint(1,100)
      pk = random.randint(1,100)
      while rid in used_rids:
        rid = random.randint(1,100)
      while pk in used_pks:
        pk = random.randint(1, 100)
      used_rids.add(rid)
      used_pks.add(pk)
      # }
      record = self.__record_builder(rid, *[pk,2,3])
      indexed_column = record[4 + 0]
      index.add(record)
      # Makes sure the RID is in the right place
      self.assertTrue(indexed_column in index.indices[0])
      self.assertTrue(rid in index.locate(0,pk))
      records.append(record)
    # Now test deleting
    for record in records:
      # record[4] is the indexed column
      pk = record[4]
      # Make sure it is in there before you delete it
      self.assertTrue(pk in index.indices[0])
      index.delete(record)
      self.assertTrue(pk not in index.indices[0])

  def test_add_and_delete_duplicates(self):
    table = Table("test_table", 3, 0)
    index = table.index
    used_rids = set()
    records = []
    for i in range(20):
      # Generating unique RIDs and duplicate primary keys {
      rid = random.randint(1, 100)
      pk = 5
      while rid in used_rids:
        rid = random.randint(1, 100)
      used_rids.add(rid)
      # }
      record = self.__record_builder(rid, *[pk, 2, 3])
      indexed_column = record[4 + 0]
      index.add(record)
      self.assertTrue(indexed_column in index.indices[0])
      self.assertTrue(rid in index.locate(0, pk))
      records.append(record)
    # Deleting, set should be gone only after all the RIDs inside are gone.
    for i, record in enumerate(records):
      # record[4] is the indexed column
      pk = record[4]
      rid = record[config.RID_COLUMN]
      # Make sure it is in there before you delete it
      self.assertTrue(pk in index.indices[0])
      index.delete(record)
      if i == 19:
        # After the last delete, the primary key shouldn't be there anymore
        self.assertTrue(pk not in index.indices[0])
      else:
        # RID should not be in the set anymore
        self.assertTrue(rid not in index.indices[0][pk])




if __name__ == '__main__':
  unittest.main()