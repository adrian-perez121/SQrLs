from lstore.table import Table
import lstore.config
import os
import pickle

from lstore.bufferpool import BufferPool

class Database():

    def __init__(self):
        self.tables = {}
        self.start_path = None

    # Required for milestone2
    def open(self, path):
        # TODO: Initialize a Bufferpool object
        if os.path.exists(path):
            print(f"Directory '{path}' already exists")
        else:
            self.start_path = path
            os.makedirs(path, exist_ok=True)
            os.makedirs(path + "/Tables", exist_ok=True)

        self.bufferpool = BufferPool(path)
        
    def close(self):
        # TODO: Run on close for bufferpool
        if hasattr(self, "bufferpool"):
            self.bufferpool.on_close()  # flush

            print("All pages flushed to disk.")
        print("Database closed successfully.")


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        if name in self.tables:
            raise  "Table already exists"

        table = Table(name, num_columns, key_index)
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
