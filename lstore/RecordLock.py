import threading

class RecordLock:
    def __init__(self):
        self.lock = threading.Lock()
        self.readers = 0
        self.writer = False

    def acquire_read(self):
        with self.lock:
            # You cannot get a read lock if someone is writing
            while self.writer:
                return False
            self.readers += 1
        return True

    def release_read(self):
        with self.lock:
            self.readers -= 1

    def acquire_write(self):
        with self.lock:
            # You shouldn't be able to get a write lock while someone is reading
            if self.readers > 0 or self.writer:
                return False
            self.writer = True
        return True

    def release_write(self):
        with self.lock:
            self.writer = False
