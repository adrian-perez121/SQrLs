from collections import defaultdict
from typing import List, Optional
import threading

class LockState:

	def __init__(self):
		self.lock: threading.Lock = threading.Lock()
		self.readers: int = 0
		self.writing: bool = False

	def increment_readers(self) -> bool:
		can_read: bool = False
		with self.lock:
			if not self.writing:
				self.readers += 1
				can_read = True
		return can_read

	def decrement_readers(self):
		with self.lock:
			self.readers -= 1

	def set_writing(self) -> bool:
		can_write: bool = False
		with self.lock:
			if self.readers == 0 and not self.writing:
				# No Readers Active AND Not Writing
				self.writing = True
				can_write = True
			elif self.readers < 0:
				print("Unexpected State for Reader Count < 0")
		return can_write

	def clear_writing(self):
		with self.lock:
			self.writing = False

class S2PLock:
	
	def __init__(self):
		self.lockStates: defaultdict[int, LockState] = defaultdict(LockState)
		self.rLocks: defaultdict[int, threading.RLock] = defaultdict(threading.RLock)
		self.locks: defaultdict[int, threading.Lock] = defaultdict(threading.Lock)
		pass
	
	def acquire(self, id: int, read: bool, transaction_locks: Optional[List[tuple[bool, LockState]]] = None) -> bool:
		# For only one thread
		if transaction_locks is None:
			return True
		state: LockState = self.lockStates[id]
		available: bool = state.increment_readers() if read else state.set_writing()

		if not available:
			return False

		transaction_locks.append((read, state))
		return True
	
	@staticmethod
	def release(transaction_locks: List[tuple[bool, LockState]]):
		# Reverse Lock Order for Safe Release
		transaction_locks.reverse()
		for read, state in transaction_locks:
			if read:
				state.decrement_readers()
			else:
				state.clear_writing()