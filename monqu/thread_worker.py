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
        if threads < 1:
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

    # test local queue and watch
    def _bulk_worker(self, order: str):
        if order == "fifo":
            get_func = self.bulk_fifo
        elif order == "lifo":
            get_func = self.bulk_lifo
        else:
            get_func = self.bulk_random

        while True:
            if func := get_func():
                self._local_queue.append(func)

            elif func is None and self._local_queue == []:
                self.watch()

            else:
                with ThreadPoolExecutor(max_workers=self.threads) as executor:
                    executor.map(self.bulk_call_funcs, self._local_queue)

    # see if range(self.prefetch - len(self._local_queue) or break is needed with map, and if on async if fill local
    # queue while working
    def _single_worker(self, order: str):
        # add timer for func timeout while running
        # Add pause logic
        if order == "fifo":
            get_func = self.bulk_fifo
        elif order == "lifo":
            get_func = self.bulk_lifo
        else:
            get_func = self.bulk_random

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

    # name equal host name or increment 1 from worker collection
    def manager(self, name: str, order: str = "fifo"):
        if order not in ["fifo", "lifo", "random"]:
            raise ValueError("order is not a correct value")

        if self._is_replica_set:
            run = self._bulk_worker
        else:
            run = self._single_worker

        run(order)
