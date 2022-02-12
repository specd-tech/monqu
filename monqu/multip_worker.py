from base_worker import BaseWorker
from os import cpu_count
from concurrent.futures import ProcessPoolExecutor


class ProcessWorker(BaseWorker):
    def __init__(
        self,
        mongo_connection: str,
        database: str = "monqu",
        queue: str = "queue",
        processes: int = cpu_count() if cpu_count() else 2,
        prefetch: int = 0,
    ):
        if processes < 1:
            raise ValueError("processes must be greater than 0")
        self.processes = processes

        if prefetch < 0:
            raise ValueError("prefetch must be greater than or equal to 0")
        super().__init__(
            mongo_connection=mongo_connection,
            database=database,
            queue=queue,
            prefetch=prefetch + self.processes,
        )

    def worker(self, order: str = "fifo"):
        # add timer
        # Add pause logic
        if order == "fifo" and self._is_replica_set:
            get_func = self.bulk_fifo
            call = self.bulk_call_funcs
        elif order == "fifo" and not self._is_replica_set:
            get_func = self.fifo
            call = self.call_func
        elif order == "random" and self._is_replica_set:
            get_func = self.bulk_random
            call = self.bulk_call_funcs
        elif order == "random" and not self._is_replica_set:
            get_func = self.random
            call = self.call_func
        else:
            raise ValueError("order is not a correct value")

        while True:
            for _ in range(self.prefetch - len(self._local_queue)):
                if func := get_func():
                    self._local_queue.append(func)

                elif func is None and self._local_queue == []:
                    self.watch()

                else:
                    break

            with ProcessPoolExecutor(max_workers=self.processes) as executor:
                executor.map(call, self._local_queue)


