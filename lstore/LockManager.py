from lstore.RecordLock import RecordLock

class LockManager:
    def __init__(self):
        self.locks = {}
        pass

    def acquire_lock(self, key, lock_type):
        if key not in self.locks:
            self.locks[key] = RecordLock()
        if lock_type == 'read':
            return self.locks[key].acquire_read()
        elif lock_type == 'write':
            return self.locks[key].acquire_write()
        return False

    def release_lock(self, key, lock_type):
        if key not in self.locks:
            return False
        if lock_type == 'read':
            self.locks[key].release_read()
            return True
        elif lock_type == 'write':
            self.locks[key].release_write()
            return True
        return False