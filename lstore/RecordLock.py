import threading

class RecordLock:
    def __init__(self):
        self.lock: threading.Lock = threading.Lock()
        self.readers: int = 0
        self.writer: bool = False

    def acquire_read(self):
        with self.lock:
            # Block acquire read until self.writer is true, however this can never happen
            while self.writer:
                return False
            self.readers += 1
        return True

    def release_read(self):
        with self.lock:
            self.readers -= 1

    def acquire_write(self):
        with self.lock:
            # You shouldn't be able to get a write lock while someone is reading or writing
            if self.readers > 0 or self.writer:
                return False
            self.writer = True
        return True

    def release_write(self):
        with self.lock:
            self.writer = False
