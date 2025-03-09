from lstore.table import Table, Record
from lstore.index import Index
import threading

class TransactionWorker(threading.Thread):

    """
    # Creates a transaction worker object.
    """
    def __init__(self, lock_manage, transactions = []):
        super().__init__()
        self.lock_manage = lock_manage 
        self.transactions = transactions
        self.result = 0 # tracks successful commits
        self.stats = [] # stores commit/abort stats


    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)


    """
    Runs all transaction as a thread
    """
    def run(self):
        for transaction in self.transactions:
            success = transaction.run()
            self.stats.append(success)
            self.result += int(success)  # +1 if committed
