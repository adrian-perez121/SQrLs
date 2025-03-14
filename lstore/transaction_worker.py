import threading
from collections import deque

class TransactionWorker:
    """
    # Creates a transaction worker object.
    """
    def __init__(self):
        self.transactions = deque()
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
        while(len(self.transactions)!=0):
            transaction = self.transactions.popleft() # gets first xact in queue
            success = transaction.run() # run transaction
            self.stats.append(success) # log stats

            # print("checking xact")
            if(success==False): # if aborted
                # print("aborted, moving xact to back")
                self.transactions.append(transaction) # adds xact to end of queue

        self.result = len(list(filter(lambda x: x, self.stats)))

    """
    Waits for the worker to finish
    """
    def join(self):
        if self._thread:
            self._thread.join()