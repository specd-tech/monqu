from SECRET import MONGO_URI
from gevent.monkey import patch_all; patch_all()
from gevent import wait
from gevent.pool import Pool
from base_worker import BaseWorker


class GeventWorker(BaseWorker):
    def __init__(
            self,
            mongo_connection: str,
            database: str = 'monqu',
            queue: str = 'queue',
            greenlet_threads: int = 10,
            prefetch: int = 0
    ):
        super().__init__(mongo_connection, database, queue)
        self.task_pool = Pool(greenlet_threads)
        # change to task pool
        self._task_pool = list()
        # add or * greenlet_threads for prefetch
        self.prefetch = greenlet_threads + prefetch
        # self.prefetch_pool = Pool(self.prefetch)
        # self._prefetch_pool = list()

    def wait(self):
        wait(self._task_pool)

    def worker(self, order: str = 'fifo'):
        # add timer
        # add patterning matching
        # Add pause logic
        get_func = self.fifo
        while True:
            left = self.prefetch - len(self._local_queue)

            for _ in range(left):
                if func := get_func():
                    self._local_queue.append(func)

                elif func is None and self._local_queue == []:
                    self.watch()

                else:
                    break

            for func in self._local_queue:
                self._task_pool.append(self.task_pool.spawn(self.call_func, func))
                self._local_queue.remove(func)
                # Test to see if this loop blocks while waiting and if this is nessary
                # maybe put at top of for loop
                if self.task_pool.full():
                    break


mq = GeventWorker(MONGO_URI, greenlet_threads=10, prefetch=20)
mq.worker()
mq.wait()
