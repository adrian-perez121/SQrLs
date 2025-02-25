from lstore.bufferpool import BufferPool
from lstore.table import Table
import lstore.config

class Database():

    def __init__(self):
<<<<<<< Updated upstream
        self.tables: dict[str, Table] = {}
=======
        self.tables = {}
        self.start_path = None
        self.bufferpool = None
>>>>>>> Stashed changes

    def open(self, path):
        # TODO: Initialize a Bufferpool object
<<<<<<< Updated upstream
        pass
=======
        self.start_path = path
        self.bufferpool = BufferPool(self.start_path)
        # TODO: get all frames that were previously active on the last run

        if os.path.exists(path):
            with open(path + "/table_names", "rb") as file:
                self.tables = pickle.load(file)
        else:
            os.makedirs(path, exist_ok=True)
            os.makedirs(path + "/Tables", exist_ok=True)
>>>>>>> Stashed changes

    def close(self):
        # TODO: Run on close for bufferpool
        pass

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
