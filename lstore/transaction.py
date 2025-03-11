
from lstore.LockManager import RWLock
from lstore.query import Query



class Transaction:
    """
    Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.table = None
        self.read_locks = set()
        self.write_locks = set()
        self.insert_locks = set()

    """
    Adds the given query to this transaction.
    Example:
      q = Query(grades_table)
      t = Transaction()
      t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        if self.table is None:
            self.table = table  # Set table reference for transaction

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        try:
            # Acquire necessary locks for queries
            for query, args in self.queries:
                key = args[0]

                if key not in self.table.lock_manager:
                    self.insert_locks.add(key)
                    self.table.lock_manager[key] = RWLock()

                if key not in self.write_locks and key not in self.insert_locks:
                    if self.table.lock_manager[key].acquire_wlock():
                        self.write_locks.add(key)
                    else:
                        return self.abort()  # Abort if write lock fails

            # Execute queries after acquiring locks
            for query, args in self.queries:
                result = query(*args)
                if not result:
                    return self.abort()

            return self.commit()

        except Exception as e:
            print(f"Transaction failed due to exception: {e}")
            return self.abort()

    """
    Rolls back the transaction, releasing all acquired locks.
    """
    def abort(self):
        for key in self.read_locks:
            self.table.lock_manager[key].release_rlock()
        for key in self.write_locks:
            self.table.lock_manager[key].release_wlock()
        for key in self.insert_locks:
            del self.table.lock_manager[key]  # Cleanup inserted records
        return False

    """
    Commits the transaction and releases all acquired locks.
    """
    def commit(self):
        for query, args in self.queries:
            query(*args)  # Execute queries

            # If deleting a record, remove its lock
            if query == Query.delete:
                key = args[0]
                if key in self.table.lock_manager:
                    del self.table.lock_manager[key]
                self.insert_locks.discard(key)
                self.write_locks.discard(key)

        # Release all locks after successful execution
        for key in self.read_locks:
            self.table.lock_manager[key].release_rlock()
        for key in self.write_locks:
            self.table.lock_manager[key].release_wlock()
        for key in self.insert_locks:
            self.table.lock_manager[key].release_wlock()
        return True