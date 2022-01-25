from base_worker import BaseWorker
from os import cpu_count
from concurrent.futures import ThreadPoolExecutor


class ThreadWorker(BaseWorker):
    def __init__(
        self,
        mongo_connection: str,
        database: str = "monqu",
        queue: str = "queue",
        # Best thread count? same as executor
        threads: int = (cpu_count() or 2) + 4,
        prefetch: int = 0,
    ):
        super().__init__(mongo_connection, database, queue)
        if threads <= 0:
            raise ValueError("threads must be greater than 0")
        # Does threads need to be class variable
        self.threads = threads
        if prefetch < 0:
            raise ValueError("prefetch must be greater than or equal to 0")
        self.prefetch = self.threads + prefetch

    def worker(self, order: str = "fifo"):
        # add timer
        # add patterning matching
        # Add pause logic
        get_func = self.fifo
        while True:
            for _ in range(self.prefetch - len(self._local_queue)):
                if func := get_func():
                    self._local_queue.append(func)

                elif func is None and self._local_queue == []:
                    self.watch()

                else:
                    break
            # max worker
            with ThreadPoolExecutor() as executor:
                executor.map(self.call_func, self._local_queue)
