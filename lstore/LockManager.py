from lstore.index import Index
import threading



class Read_Write_Lock:

	def __init__(self):
		self.lock = threading.Lock()
		self.reader = 0
		self.writer = False

	def acquire_read(self):
		self.lock.acquire()
		ret = None
		with self.lock:
			if self.writer:
				ret = False
			else:
				self.reader += 1
				ret = True

		return ret
	def release_read(self):
		with self.lock:
			self.reader -= 1

	def acquire_write(self):
		ret = None
		with self.lock:
			if self.reader or self.writer:
				ret = False
			else:
				self.writer = True
				ret = True

		return ret

	def release_write(self):
		with self.lock:
			self.writer = False