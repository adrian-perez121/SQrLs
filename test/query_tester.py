import unittest

import sys
import random

from lstore.table import Table
import lstore.config as config
from lstore.query import Query

sys.path.append('../lstore')
import unittest

class MyTestCase(unittest.TestCase):
  def test_insert(self):
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
    records = []
    for i in range(20):
      # Generating a random set of unique RIDs and PKs {
      pk = random.randint(1, 100)
      while pk in used_pks:
        pk = random.randint(1, 100)
      used_rids.add(rid)
      used_pks.add(pk)
      # }
      record = [pk, 2, 3]
      query.insert(*record)
      # Check index has the right value
      self.assertEqual(index.indices[0][pk], rid)
      self.assertTrue(rid in page_directory) # Not going to check the the values of this just yet
      rid += 1


if __name__ == '__main__':
    unittest.main()
