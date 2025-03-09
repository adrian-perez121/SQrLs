from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock_manage import LockManage

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self, lock_manage):
        self.queries = []
        # locks
        self.lock_manage = lock_manage
        self.transaction_id = id(self)

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
        acquired_locks = [] # track for rollback
        try:
            for query, args in self.queries:
                rid = args[0]
                lock_type = "E" if query.__name__ in ["insert", "update", "delete"] else "S"

                # acquire lock
                if not self.lock_manage.acquire_record_lock(rid, self.transaction_id, lock_type):
                    raise Exception(f"Transaction {self.transaction_id} failed to acquire a lock on {rid}")
                acquired_locks.append(rid)
                # run actual query
                result = query(*args)

                if result is False:
                    raise Exception(f"Transaction {self.transaction_id} aborted from query failure")
                
            return self.commit(acquired_locks)
        except Exception as e:
            print(e)
            return self.abort(acquired_locks)
    
    def abort(self, acquired_locks):
        """ Release locks / rollbacks """
        print(f"Transaction {self.transaction_id} aborted | Rolling back")
        for rid in acquired_locks:
            self.lock_manage.release_record_lock(rid, self.transaction_id)
        return False

    
    def commit(self, acquired_locks):
        """ Release locks / commit transaction """
        print(f"Transaction {self.transaction_id} committed")
        for rid in acquired_locks:
            self.lock_manage.release_record_lock(rid, self.transaction_id)
        return True
