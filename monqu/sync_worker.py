from base_worker import BaseWorker


class SyncWorker(BaseWorker):
    def __init__(
        self,
        mongo_connection: str,
        database: str = "monqu",
        queue: str = "queue",
        prefetch: int = 1,
    ):
        super().__init__(
            mongo_connection=mongo_connection,
            database=database,
            queue=queue,
            prefetch=prefetch,
        )

    # Make type hint for fifo, random, and stack
    def worker(self, order: str = "fifo"):
        # match order:
        #     case "fifo":
        #         get_func = self.fifo
        #     case 404:
        #         return "Not found"
        #     case _:
        #         raise ValueError("order is not a correct value")
        get_func = self.fifo
        while True:
            if func := get_func():
                self.call_func(func)

            else:
                self.watch()
