from gevent.monkey import patch_all

patch_all()
from os import cpu_count
from gevent import wait
from gevent.pool import Pool
from base_worker import BaseWorker


class GeventWorker(BaseWorker):
    def __init__(
        self,
        mongo_connection: str,
        database: str = "monqu",
        queue: str = "queue",
        # Best thread count? same as executor
        greenlet_threads: int = (cpu_count() or 2) + 4,
        prefetch: int = 0,
    ):
        if greenlet_threads <= 0:
            raise ValueError("greenlet_threads must be greater than 0")

        if prefetch < 0:
            raise ValueError("prefetch must be greater than or equal to 0")
        super().__init__(
            mongo_connection=mongo_connection,
            database=database,
            queue=queue,
            prefetch=greenlet_threads + prefetch,
        )
        # Rename task_pool and _task_pool
        self.task_pool = Pool(greenlet_threads)
        # change to task pool
        self._task_pool = list()
        # self.prefetch_pool = Pool(self.prefetch)
        # self._prefetch_pool = list()

    def wait(self):
        wait(self._task_pool)

    def worker(self, order: str = "fifo"):
        # add timer
        # add patterning matching
        # Add pause logic
        get_func = self.fifo
        while True:
            # Fix so it works with 0 prefetch for other worker types
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
