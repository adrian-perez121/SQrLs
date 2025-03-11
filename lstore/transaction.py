from lstore.table import Table
from lstore.record import Record
from lstore.index import Index
from lstore.lock_manager import LockManager
from lstore.query import Query


class Transaction:

    """
    Creates a transaction object.
    """
    lock_manager = LockManager()

    def __init__(self):
        self.queries = []
        self.locked_rids = []
        self.write_set = {}
        self.required_resources = set()

    def get_required_resources(self):
        """Returns the set of resources this transaction requires."""
        return self.required_resources

    def add_required_resource(self, resource_id):
        """Adds a required resource for the transaction."""
        self.required_resources.add(resource_id)

    """
    Adds the given query to this transaction.
    """
    def add_query(self, query, table, rid, *args):
        self.queries.append((query, table, rid, args))

    """
    Runs all queries in the transaction, ensuring ACID properties.
    Returns True if transaction commits, False on abort.
    """

    def run(self):
        query_executor = Query(self.queries[0][1])  # Initialize Query with the table

        for query, table, rid, args in self.queries:
            if not Transaction.lock_manager.acquire_lock(rid, "X", id(self)):
                return self.abort()

            # Use Query object instead of calling select on Table directly
            old_record = query_executor.select(rid, 0, [1] * table.num_columns)
            if old_record:
                self.write_set[rid] = old_record[0].columns[:]

            result = query(*args)
            if result is False:
                return self.abort()

            self.locked_rids.append(rid)

        return self.commit()

    def abort(self):
        # Rollback changes
        for rid, old_values in self.write_set.items():
            table = self.queries[0][1]  # Get the table reference
            table.update(rid, *old_values)

        # Release all locks
        for rid in self.locked_rids:
            Transaction.lock_manager.release_lock(rid, id(self))

        return False

    def commit(self):
        # Commit transaction
        for rid in self.locked_rids:
            Transaction.lock_manager.release_lock(rid, id(self))
        return True
