from lstore.table import Table
import lstore.config

class Database():

    def __init__(self):
        self.tables = {}

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
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
