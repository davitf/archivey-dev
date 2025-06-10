
import multiprocessing
import threading


class GlobalUniqueIdGenerator:
    def __init__(self, batch_size=1000):
        self._batch_size = batch_size
        self._global_counter = multiprocessing.Value('i', 1)
        self._lock = threading.Lock()

    def next_id(self):
        with self._lock, self._global_counter.get_lock():
            value = self._global_counter.value
            self._global_counter.value += 1
            return value

_global_generator = GlobalUniqueIdGenerator()
_BATCH_SIZE = 1000

class UniqueIdGenerator:
    def __init__(self):
        self._local_count = _BATCH_SIZE
        self._batch_start = -100000000
        self._lock = threading.Lock()

    def next_id(self):
        with self._lock:
            if self._local_count >= _BATCH_SIZE:
                self._batch_start = _global_generator.next_id() * _BATCH_SIZE
                self._local_count = 0

            assert self._batch_start >= 0
            value = self._batch_start + self._local_count
            self._local_count += 1
            return value


UNIQUE_ID_GENERATOR = UniqueIdGenerator()

