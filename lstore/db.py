import json
import pickle
import os
from lstore.bufferpool import BufferPool
from lstore.table import Table
from lstore.index import Index

class Database():

    def __init__(self):
        self.tables: dict[str, Table] = {}
        self.start_path = None
        self.bufferpool: BufferPool = None

    def open(self, path):
        self.start_path = path
        self.bufferpool = BufferPool(self.start_path)

    def close(self):
        self.bufferpool.on_close()
        if self.start_path:
            for table in self.tables.values():
                # save index as json
                with open(self.start_path + f"/tables/{table.name}/index.json", 'w') as file:
                    json.dump(table.index.to_arr(), file)

                # create clean copy
                table_copy = Table(table.name, table.num_columns, table.key, None)
                table_copy.index = None  # prevent json issues
                table_copy.bufferpool = None  # prevent pickling errors
                table_copy.page_directory = table.page_directory  # keep page mappings

                # remove problematic lock attributes
                if hasattr(table_copy, "lock_manage"):
                    table_copy.lock_manage = None  # removes _thread.lock
                if hasattr(table_copy, "some_other_lock"):  # safety
                    table_copy.some_other_lock = None

                # pickle the clean table
                with open(self.start_path + f"/tables/{table.name}/metadata.pkl", 'wb') as file:
                    pickle.dump(table_copy, file)



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
        table_path = f"{self.start_path}/tables/{name}/"
        if os.path.exists(table_path):
            with open(f"{table_path}metadata.pkl", 'rb') as file:
                table = pickle.load(file)
                table.bufferpool = self.bufferpool
                self.tables[name] = table
                with open(f"{table_path}index.json", 'r') as file:
                    table.index = Index.from_arr(table, json.load(file))
                return table
