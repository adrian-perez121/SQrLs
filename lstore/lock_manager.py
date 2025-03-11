import threading
import logging



class LockManager:
	def __init__(self):
		self.locks = {}
		self.lock_table = {}
		self.lock_table_lock = threading.Lock()

	def acquire_lock(self, transaction, resource_id, lock_type=None, timeout=None):
		"""Acquires a lock on a specific resource, supporting optional lock type and timeout"""
		logging.info(f"Acquiring {lock_type} lock on {resource_id} for transaction {id(transaction)}")

		with self.lock_table_lock:
			if resource_id not in self.locks:
				self.locks[resource_id] = threading.Lock()

			acquired = self.locks[resource_id].acquire(timeout=timeout)
			if acquired:
				self.lock_table[resource_id] = (lock_type, id(transaction))
				return True
			else:
				logging.warning(f"Transaction {id(transaction)} failed to acquire lock on {resource_id}")
				return False

	def release_lock(self, rid, transaction_id):
		with self.lock_table_lock:
			if rid in self.lock_table and self.lock_table[rid][1] == transaction_id:
				self.locks[rid].release()
				del self.lock_table[rid]