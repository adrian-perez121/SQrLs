import threading

class LockManage:
    def __init__(self):
        self.record_lock = {} # {RID: {"lock": RLock(), "lock_type": "S" or "E", "transactions": {transaction_id1, transaction_id2}}}
        self.page_lock = {} # {PageID: threading.Lock()}
        self.lock_mutex = threading.Lock() # ensure thread-safe access within the dictionary

    def acquire_record_lock(self, rid, transaction_id, lock_type):
        """ Acquire a (S)HARED or (E)XCLUSIVE lock in a record """
        with self.lock_mutex:
            # init if !exist
            if rid not in self.record_lock:
                self.record_lock[rid] = {"lock": threading.RLock(), "lock_type": None, "transactions": set()}

            lock_entry = self.record_lock[rid]

            if lock_type == "S":
                if lock_entry["lock_type"] in [None, "S"]: # we allow if !E
                    lock_entry["transactions"].add(transaction_id)
                    lock_entry["lock_type"] = "S"
                    return True
                return False # cant get S lock if a E lock exists
            elif lock_type == "E":
                if lock_entry["lock_type"] is None or (lock_entry["lock_type"] == "S" and len(lock_entry["transactions"]) == 0): # no locks held
                    lock_entry["lock_type"] = "E"
                    lock_entry["transactions"].add(transaction_id)
                    return True
                return False # cant get if ANY lock exists
            
    def release_record_lock(self, rid, transaction_id):
        """ Release lock on a record if no transactions hold it """
        with self.lock_mutex:
            if rid in self.record_lock and transaction_id in self.record_lock[rid]["transactions"]:
                self.record_lock[rid]["transactions"].remove(transaction_id)

                # if no more transactions hold the lock we can remove entry
                if not self.record_lock[rid]["transactions"]:
                    del self.record_lock[rid]
                
    def acquire_page_lock(self, page_id):
        """ Acquire page-level lock to stop concurr changes """
        with self.lock_mutex:
            if page_id not in self.page_lock:
                self.page_lock[page_id] = threading.Lock()
            return self.page_lock[page_id].acquire(blocking=True)
    
    def release_page_lock(self, page_id):
        """ Release page-level lock """
        with self.lock_mutex:
            if page_id in self.page_lock:
                self.page_lock[page_id].release()