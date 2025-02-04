import unittest

import sys
import random

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



if __name__ == '__main__':
    unittest.main()
