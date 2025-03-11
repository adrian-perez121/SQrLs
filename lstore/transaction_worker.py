import threading
import time
import random
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class TransactionWorker:
    """
    Transaction worker with deadlock detection & handling.
    """

    lock_manager = {}  # Tracks locked resources: {rid: transaction_id}
    transaction_dependencies = {}  # Wait-for graph {transaction_id: waiting_on_transaction_id}

    def __init__(self):
        self.stats = []
        self.transactions = []
        self.result = 0
        self.resources = 0
        self.thread = None
        self.max_retries = 5  # Maximum retry attempts for deadlock handling

    def get_required_resources(self):
        return self.resources

    def add_transaction(self, t):
        """
        Adds a transaction to the worker.
        """
        self.transactions.append(t)

    def run(self):
        """
        Runs transactions in a separate thread.
        """
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()

    def join(self):
        """
        Waits for the worker thread to finish.
        """
        if self.thread:
            self.thread.join()

    def __run(self):
        """
        Executes transactions with deadlock detection & handling.
        """
        for transaction in self.transactions:
            retries = 0
            while retries < self.max_retries:
                try:
                    # Attempt to acquire locks for the transaction
                    locked = True
                    for resource_id in transaction.get_required_resources():
                        locked = self.acquire_lock(transaction, resource_id, lock_type="exclusive", timeout=0.5)
                        if not locked:
                            logging.warning(f"Deadlock detected! Aborting Transaction {id(transaction)}.")
                            self.abort_transaction(id(transaction))
                            retries += 1
                            time.sleep(random.uniform(0.05, 0.2))  # Randomized backoff
                            break
                    if not locked:
                        continue

                    success = transaction.run()
                    self.stats.append(success)

                    if success:
                        logging.info(f"Transaction {id(transaction)} committed successfully.")
                        self.release_locks(transaction)
                        break  # Exit retry loop if successful
                    else:
                        logging.warning(f"Transaction {id(transaction)} failed. Retrying {retries + 1}/{self.max_retries}...")
                        retries += 1
                        time.sleep(0.1)

                except Exception as e:
                    logging.error(f"Transaction {id(transaction)} encountered an error: {e}")
                    retries += 1
                    time.sleep(0.1)

        self.result = len([x for x in self.stats if x])  # Count successful transactions

    def acquire_lock(self, transaction, resource_id, lock_type=None, timeout=None):
        """Acquires a lock on a specific resource, supporting optional lock type and timeout"""
        logging.info(f"Acquiring {lock_type} lock on {resource_id} for transaction {id(transaction)}")

        if not hasattr(transaction, "get_required_resources"):
            logging.error(f"Transaction {id(transaction)} does not have 'get_required_resources' method.")
            return False

        with threading.Lock():
            if resource_id in self.lock_manager:
                waiting_on = self.lock_manager[resource_id]
                self.transaction_dependencies[id(transaction)] = waiting_on

                if self.detect_deadlock(transaction):
                    logging.warning(f"Deadlock detected! Aborting Transaction {id(transaction)}.")
                    return False

            self.lock_manager[resource_id] = id(transaction)
        return True

    def release_locks(self, transaction):
        """
        Releases all locks held by a transaction.
        """
        for rid in list(self.lock_manager.keys()):
            if self.lock_manager[rid] == id(transaction):
                del self.lock_manager[rid]

        if id(transaction) in self.transaction_dependencies:
            del self.transaction_dependencies[id(transaction)]

    def detect_deadlock(self, transaction):
        """Detects cycles in the transaction dependency graph to resolve deadlocks."""
        visited = set()
        current = id(transaction)

        while current in self.transaction_dependencies:
            if current in visited:
                logging.warning(f"Deadlock detected! Transaction {current} is in a cycle.")

                # Choose the youngest transaction in the cycle to abort
                youngest_transaction = max(visited, key=lambda tid: tid)
                logging.warning(f"Aborting youngest transaction {youngest_transaction} to resolve deadlock.")

                # Mark for abort and release locks
                self.abort_transaction(youngest_transaction)
                return True

            visited.add(current)
            current = self.transaction_dependencies[current]

        return False  # No deadlock detected

    def abort_transaction(self, transaction_id):
        """Aborts a transaction and releases all its acquired locks."""
        logging.warning(f"Transaction {transaction_id} aborted to resolve deadlock.")

        # Release all locks held by the transaction
        locks_to_release = [rid for rid, tid in self.lock_manager.items() if tid == transaction_id]
        for rid in locks_to_release:
            del self.lock_manager[rid]

        # Remove transaction dependencies
        if transaction_id in self.transaction_dependencies:
            del self.transaction_dependencies[transaction_id]
