from SECRET import MONGO_URI
from base_worker import BaseWorker
from multiprocessing import Pool, cpu_count
from time import time
from concurrent.futures import ThreadPoolExecutor


class ProcessWorker(BaseWorker):
    def __init__(
        self,
        mongo_connection: str,
        database: str = "monqu",
        queue: str = "queue",
        processes: int = cpu_count() if cpu_count() else 2,
        prefetch: int = 0,
    ):
        super().__init__(mongo_connection, database, queue)
        if processes <= 0:
            raise ValueError("processes must be greater than 0")
        self.processes = processes
        self.task_pool = Pool(self.processes)
        if prefetch < 0:
            raise ValueError("prefetch must be greater than or equal to 0")
        self.prefetch = self.processes + prefetch

    def wait(self):
        self.task_pool.close()
        self.task_pool.join()

    def worker(self, order: str = "fifo"):
        # add timer
        # add patterning matching
        # Add pause logic
        get_func = self.fifo
        while True:
            print("Loop")
            left = self.prefetch - len(self._local_queue)
            print(f"Left: {left}")
            for _ in range(left):
                print("Range")
                if func := get_func():
                    self._local_queue.append(func)

                elif func is None and self._local_queue == []:
                    self.watch()

                else:
                    break
            print("Calling Processes")
            for func in self._local_queue:
                self.task_pool.apply_async(self.call_func, func)
                self._local_queue.remove(func)
                print(func)
                # Test to see if this loop blocks while waiting and if this is nessary
                # if self.task_pool.:
                #     break


if __name__ == "__main__":
    mq = ProcessWorker(MONGO_URI, processes=4, prefetch=0)
    t0 = time()
    mq.worker()
    mq.wait()
    t1 = time()
    print(t1 - t0)
