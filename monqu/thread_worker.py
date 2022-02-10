from base_worker import BaseWorker
from os import cpu_count
from concurrent.futures import ThreadPoolExecutor


class ThreadWorker(BaseWorker):
    def __init__(
        self,
        mongo_connection: str,
        database: str = "monqu",
        queue: str = "queue",
        threads: int = (cpu_count() or 2) + 4,
        prefetch: int = 0,
    ):
        if threads <= 0:
            raise ValueError("threads must be greater than 0")
        self.threads = threads

        if prefetch < 0:
            raise ValueError("prefetch must be greater than or equal to 0")
        super().__init__(
            mongo_connection=mongo_connection,
            database=database,
            queue=queue,
            prefetch=prefetch + self.threads,
        )

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

            with ThreadPoolExecutor(max_workers=self.threads) as executor:
                executor.map(self.call_func, self._local_queue)
