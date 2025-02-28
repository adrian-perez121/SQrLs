import pickle
import os
from lstore.bufferpool import BufferPool
from lstore.table import Table

class Database():

    def __init__(self):
        self.tables: dict[str, Table] = {}
        self.start_path = None
        self.bufferpool: BufferPool = None

    def open(self, path):
        self.start_path = path
        self.bufferpool = BufferPool(self.start_path)
        tables_path: str = self.start_path + "/tables/pickle.pkl"
        if os.path.exists(tables_path):
            with open(tables_path, 'rb') as file:
                self.tables = pickle.load(file)

    def close(self):
        self.bufferpool.on_close()
        if self.start_path:
            with open(self.start_path + "/tables/pickle.pkl", 'wb') as file:
                pickle.dump(self.tables, file)

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name: str, num_columns: int, key_index: int):
        if name in self.tables:
            raise  "Table already exists"

        table = Table(name, num_columns, key_index, self.bufferpool)
        self.tables[name] = table
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name in self.tables:
            del self.tables[name]
        else:
            raise "Table does not exist"

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables[name]
