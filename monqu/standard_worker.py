from SECRET import MONGO_URI
from base_worker import BaseWorker


class StandardWorker(BaseWorker):
    def __init__(
            self,
            mongo_connection: str,
            database: str = 'monqu',
            queue: str = 'queue',
            prefetch: int = 0
    ):
        super().__init__(mongo_connection, database, queue)
        self.prefetch = prefetch

    def worker(self, order: str = 'fifo'):
        # add timer
        # add patterning matching
        # Add pause logic

        # get_func should be turned in to pattern match
        get_func = self.fifo
        while True:
            left = self.prefetch - len(self._local_queue)

            for _ in range(left):
                if func := get_func():
                    self._local_queue.append(func)

                elif func is None and self._local_queue == []:
                    self.watch()

                else:
                    break

            for func in self._local_queue:
                self.call_func(func)
                self._local_queue.remove(func)


mq = StandardWorker(MONGO_URI)
mq.worker()
