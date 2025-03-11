from lstore.query import Query
from lstore.table import Table
from lstore.record import Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        # rids for complete transactions, held in case of an abort later on
        # Each entry is a tuple of (query_type, rid, pk)
        self.stack = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        # use grades_table for aborting


    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            # if query.__name__ == "insert":
            #     # If the query is an insert, we need to store the rid and pk
            #     rid = result
            #     pk = args[0]
            #
            # if query.__name__ == "update":
            #     # If the query is an update, we need to store the rid and pk
            #     rid = result
            #     pk = args[0]
            #
            # if query.__name__ == "delete":
            #     # If the query is a delete, we need to store the rid and pk
            #     rid = result
            #     pk = args[0]

        return self.commit()


    def abort(self):
        #TODO: do roll-back and any other necessary operations
        return False


    def commit(self):
        # TODO: commit to database
        return True

