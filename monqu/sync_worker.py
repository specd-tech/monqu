from base_worker import BaseWorker


class SyncWorker(BaseWorker):
    def __init__(
        self,
        mongo_connection: str,
        database: str = "monqu",
        queue: str = "queue",
        prefetch: int = 0,
    ):
        if prefetch < 0:
            raise ValueError("prefetch must be greater than or equal to 0")
        super().__init__(
            mongo_connection=mongo_connection,
            database=database,
            queue=queue,
            prefetch=prefetch + 1,
        )

    # Make type hint for fifo, random, and stack
    def worker(self, order: str = "fifo"):
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
            if funcs := get_func():
                call(funcs)

            else:
                self.watch()
