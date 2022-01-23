from base_worker import BaseWorker


class ThreadWorker(BaseWorker):
    def __init__(
            self,
            mongo_connection: str,
            database: str = 'monqu',
            queue: str = 'queue',
            threads: int = 10,
            prefetch: int = 0
    ):
        super().__init__(mongo_connection, database, queue)
        self.threads = threads
        # add or * greenlet_threads for prefetch
        self.prefetch = self.threads + prefetch






