import threading

class TransactionWorker:
    """
    # Creates a transaction worker object.
    """
    def __init__(self):
        self.transactions = []
        self.stats = []
        self.result = 0
        self._thread = None

    """
    Appends t to transactions
    """
    def add_transaction(self, transaction):

        self.transactions.append(transaction)

    """
    Runs all transaction as a thread
    """
    def run(self):
        self._thread = threading.Thread(target=self.__run)
        self._thread.daemon = True
        self._thread.start()

    def __run(self):
        for transaction in self.transactions:
            success = transaction.run()
            self.stats.append(success)
        self.result = len(list(filter(lambda x: x, self.stats)))

    """
    Waits for the worker to finish
    """
    def join(self):
        if self._thread:
            self._thread.join()