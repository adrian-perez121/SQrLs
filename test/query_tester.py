import unittest

import sys
import random
from selectors import KqueueSelector

from lstore.table import Table
import lstore.config as config
from lstore.query import Query

sys.path.append('../lstore')
import unittest

class MyTestCase(unittest.TestCase):
  def test_insert_and_select(self):
    # A rigorous testing of add.
    # Checks if the index was updated correctly
    # Checks if the page directory was update correctly
    table = Table("test", 3, 0)
    query = Query(table)
    page_directory = table.page_directory
    index = table.index

    used_rids = set()
    used_pks = set()
    rid = 1 # The table starts rids at 1 so this should align
    # An array of tuples to practice deleting
    for i in range(10000):
      # Generating a random set of unique RIDs and PKs {
      pk = random.randint(1, 100000)
      while pk in used_pks:
        pk = random.randint(1, 100000)
      used_rids.add(rid)
      used_pks.add(pk)
      # }
      record_data = [pk, 2, 3]
      query.insert(*record_data)
      # Check index has the right value
      self.assertTrue(rid in index.indices[0][pk])
      self.assertTrue(rid in page_directory) # Not going to check  the values of this just yet
      # This is an actual record object
      record = query.select(pk, 0, [1, 1, 1])[0]
      self.assertEqual(record_data, record.columns)
      rid += 1

  def test_insert_select_duplicate(self):
    table = Table("test", 3, 0)
    query = Query(table)
    page_directory = table.page_directory
    records_data = [[1, 2, 5], [1, 20, 50], [1, 200, 500]]
    for record_data in records_data:
      query.insert(*record_data)
    records = query.select(1, 0, [1, 1, 1])
    # This works but be careful because sets aren't ordered
    for i, record in enumerate(records):
      self.assertEqual(records_data[i], record.columns)

  def test_select_version_with_no_updates(self):
    # Testing how the method works when we only have one version of a record
    table = Table("test", 3, 0)
    query = Query(table)
    used_pks = set()
    for i in range(5):
      pk = random.randint(1, 1000)
      while pk in used_pks:
        pk = random.randint(1, 1000)
      used_pks.add(pk)
      record_data = [pk, random.randint(0,20), random.randint(0, 20)]
      query.insert(*record_data)
      record = query.select_version(pk, 0, [1, 1, 1], 0)[0]
      self.assertEqual(record_data, record.columns)

  def test_update(self):
    table = Table("test", 3, 0)
    query = Query(table)
    used_pks = set()
    rid = 1
    for i in range(5):
      pk = random.randint(1, 1000)
      while pk in used_pks:
        pk = random.randint(1, 1000)
      used_pks.add(pk)
      record_data = [pk, random.randint(0, 20), random.randint(0, 20)]
      query.insert(*record_data)
      query.update(pk, *[pk, 0, 0])
      rid += 1 # Tail RID
      self.assertTrue(rid in table.page_directory)
      record = query.select(pk, 0, [1,1,1])[0]
      self.assertEqual(record.schema_encoding, [1, 1, 1]) # Everything should have been updated
      self.assertEqual(record.indirection, rid) # Indirection should point to tail record
      rid += 1

  def test_update_same_record(self):
    table = Table("test", 4, 0)
    query = Query(table)
    query.insert(*[1,2,3,4])

    record = query.select(1, 0, [0, 0, 0, 0])[0]
    self.assertEqual(record.schema_encoding, [0, 0, 0, 0])
    self.assertEqual(record.indirection, 0)
    query.update(1, *[None, None, 5, 7])

    record = query.select(1, 0, [0, 0, 0, 0])[0]
    self.assertEqual(record.schema_encoding, [0, 0, 1, 1])
    self.assertEqual(record.indirection, 2)
    query.update(1, *[None, 5, None, None])

    record = query.select(1, 0, [0, 0, 0, 0])[0]
    self.assertEqual(record.schema_encoding, [0, 1, 1, 1])
    self.assertEqual(record.indirection, 3)

    record = query.select_version(1, 0, [1, 1, 1, 1], 0)[0]
    self.assertEqual(record.columns, [1, 5, 5, 7])

    record = query.select_version(1, 0, [1, 1, 1, 1], -1)[0]
    self.assertEqual(record.columns, [1, 2, 5, 7])

    record = query.select_version(1, 0, [1, 1, 1, 1], -2)[0]
    self.assertEqual(record.columns, [1, 2, 3, 4])

    record = query.select_version(1, 0, [1, 1, 1, 1], -10)[0]
    self.assertEqual(record.columns, [1, 2, 3, 4])



if __name__ == '__main__':
    unittest.main()