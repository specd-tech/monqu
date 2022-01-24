from base_worker import BaseWorker


class SyncWorker(BaseWorker):
    def __init__(
        self,
        mongo_connection: str,
        database: str = "monqu",
        queue: str = "queue",
    ):
        super().__init__(mongo_connection, database, queue)

    def worker(self, order: str = "fifo"):
        # add timer
        # add patterning matching
        # Add pause logic

        # get_func should be turned in to pattern match
        get_func = self.fifo
        while True:
            if func := get_func():
                self.call_func(func)

            elif func is None:
                self.watch()
