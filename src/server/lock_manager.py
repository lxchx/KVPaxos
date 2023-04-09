import threading

class LockManager:
    def __init__(self):
        self.locks = {}
        self.lock = threading.Lock()

    def get_lock(self, key: tuple) -> threading.Lock:
        with self.lock:
            # 加锁保证对同一个键的操作是原子的
            if key not in self.locks:
                self.locks[key] = threading.Lock()
                
            return self.locks[key]
