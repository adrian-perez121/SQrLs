from lstore.table import Table
import lstore.config
import os
import pickle

class Database():

    def __init__(self):
        self.tables = {}
        self.start_path = None

    # Required for milestone2
    def open(self, path):
        # TODO: Initialize a Bufferpool object
        # self.start_path = path
        #
        # if os.path.exists(path):
        #     with open(path + "/table_names", "rb") as file:
        #         self.tables = pickle.load(file)
        # else:
        #     os.makedirs(path, exist_ok=True)
        #     os.makedirs(path + "/Tables", exist_ok=True)
        pass

    def close(self):
        # # TODO: Run on close for bufferpool
        # # Write the hash into a pickle file for later
        # with open(self.start_path + "/table_names", 'wb') as file:
        #     pickle.dump(self.tables, file)
        pass

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
