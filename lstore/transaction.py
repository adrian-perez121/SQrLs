from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
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
        return self.commit()

<<<<<<< Updated upstream
    
=======
                if key not in self.table.lock_manager:
                    self.insert_locks.add(key)
                    self.table.lock_manager[key] = RWLock()

                if key not in self.write_locks and key not in self.insert_locks:
                    if self.table.lock_manager[key].acquire_wlock():
                        self.write_locks.add(key)
                    else:
                        print(f"Transaction aborted because {key not in self.write_locks} {key not in self.insert_locks}")
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
>>>>>>> Stashed changes
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        return False

    
    def commit(self):
        # TODO: commit to database
        return True

