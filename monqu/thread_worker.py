from base_worker import BaseWorker
from os import cpu_count


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
        # Does threads need to be class var
        self.threads = threads
        # add or * greenlet_threads for prefetch
        self.prefetch = self.threads + prefetch
