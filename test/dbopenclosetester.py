import sys
import os
import shutil # For removing the directory

from lstore.db import Database

sys.path.append('../lstore')

import unittest

class MyTestCase(unittest.TestCase):
  def test_paths(self):
    # Make sure the path is created when you open
    db = Database()
    db.open('./ECS165')
    db.close()
    self.assertTrue(os.path.exists("./ECS165"))
    self.assertTrue(os.path.exists("./ECS165/table_names"))

    shutil.rmtree('./ECS165') # clean up


if __name__ == '__main__':
  unittest.main()
