from lstore.index import Index
import threading




class Lock:
    
    def __init__(self):
        self.UNLOCK = 0
        self.MUTEX = 1
        self.SHARED = 2

class RWLock:

	def __init__(self):
		self.lock = threading.Lock()
		self.reader = 0
		self.writer = False

	def acquire_rlock(self):
		self.lock.acquire()

		if self.writer:
			self.lock.release()
			return False
		else:
			self.reader += 1
			self.lock.release()
			return True

	def release_rlock(self):
		self.lock.acquire()
		self.reader -= 1
		self.lock.release()

	def acquire_wlock(self):
		self.lock.acquire()

		if self.reader != 0:  # if something is reader, can't write
			self.lock.release()
			return False
		elif self.writer:  # if something else is writer, can't write
			self.lock.release()
			return False
		else:
			self.writer = True
			self.lock.release()
			return True

	def release_wlock(self):
		self.lock.acquire()
		self.writer = False
		self.lock.release()