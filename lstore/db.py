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
                table_path = os.path.join(self.start_path, "tables", table.name)
                with open(f"{table_path}index.json","w+", encoding="utf-8") as file:
                    json.dump(table.index.to_arr(), file)

                table.index = None
                with open(f"{table_path}.json", 'w+', encoding="utf-8") as file:
                    json.dump(table.to_dict(), file)

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
        # table_path = f"{self.start_path}/tables/{name}/"
        table_path = os.path.join(self.start_path, "tables", name)
        if os.path.exists(table_path):
            with open(f"{table_path}.json", 'r') as file:
                data = json.load(file)
                new_page_directory = {}
                for i, k in enumerate(data["page_directory_keys"]):
                    new_page_directory[k] = tuple(data["page_directory_values"][i])


                table = Table(name=data["name"], num_columns=data["num_columns"], key=data["key"],
                              bufferpool=self.bufferpool, page_ranges_index= data["page_ranges_index"], page_directory=new_page_directory, rid=data["rid"])
                self.tables[name] = table
                with open(f"{table_path}index.json", 'r') as file:
                    table.index = Index.from_arr(table, json.load(file))
                return table

