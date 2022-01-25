from gevent.monkey import patch_all

patch_all()
import gevent
from gevent import wait
from gevent.pool import Pool
from os import cpu_count
from cloudpickle.cloudpickle_fast import dumps
from pymongo import MongoClient
from bson import Binary
from typing import Union, Callable
from functools import wraps
from base_worker import BaseWorker
from collections import defaultdict


# flip ret and proiety
# make sure _tasks is correct and doesn't need to be Pool()
class MonquServer:
    def __init__(
        self, mongo_connection: str, database: str = "monqu", queue: str = "queue"
    ):
        self.client = MongoClient(mongo_connection)
        self.database = self.client[database]
        self.col = self.database[queue]
        self.queue = queue
        self._bulk_queue = defaultdict(list)
        self._tasks = []

    @staticmethod
    def _payload(
        func: Callable,
        args: Union[tuple, list],
        kwargs: dict,
        priority: int,
        retries: int,
    ) -> dict:
        if retries < 0:
            raise ValueError("retries must be greater than or equal to 0")
        if priority < 0:
            raise ValueError("priority must be greater than or equal to 0")
        payload = {
            "status": None,
            "priority": priority,
            "retries": retries,
            "func": Binary(dumps(func)),
        }
        if args:
            payload["args"] = Binary(dumps(args))

        if kwargs:
            payload["kwargs"] = Binary(dumps(kwargs))

        return payload

    def enqueue(
        self,
        func: Callable,
        args: Union[tuple, list] = None,
        kwargs: dict = None,
        queue: str = None,
        priority: int = 0,
        retries: int = 0,
    ):
        queue = queue if queue else self.col
        self._tasks.append(
            gevent.spawn(
                queue.insert_one, self._payload(func, args, kwargs, priority, retries)
            )
        )

    def bulk_enqueue(
        self,
        func: Callable,
        args: Union[tuple, list] = None,
        kwargs: dict = None,
        queue: str = None,
        priority: int = 0,
        retries: int = 0,
    ):
        queue = queue if queue else self.queue
        self._bulk_queue[queue] += [
            self._payload(func, args, kwargs, priority, retries)
        ]

    def bulk_insert(self):
        # Change terms?
        for queue, tasks in self._bulk_queue.items():
            self._tasks.append(gevent.spawn(self.database[queue].insert_many, tasks))

    def task(
        self,
        original_func: Callable = None,
        queue: str = None,
        priority: int = 0,
        retries: int = 1,
    ):
        def wrapper(func):
            @wraps(func)
            def enqueue_wrapper(*args, **kwargs):
                self.enqueue(func, args, kwargs, queue, priority, retries)

            return enqueue_wrapper

        if original_func:
            return wrapper(original_func)
        return wrapper

    def bulk_task(
        self,
        original_func: Callable = None,
        queue: str = None,
        priority: int = 0,
        retries: int = 1,
    ):
        def wrapper(func):
            @wraps(func)
            def bulk_enqueue_wrapper(*args, **kwargs):
                self.bulk_enqueue(func, args, kwargs, queue, priority, retries)

            return bulk_enqueue_wrapper

        if original_func:
            return wrapper(original_func)
        return wrapper

    def wait(self):
        gevent.joinall(self._tasks)


class GeventWorker(BaseWorker):
    def __init__(
        self,
        mongo_connection: str,
        database: str = "monqu",
        queue: str = "queue",
        # Best thread count? same as executor
        greenlet_threads: int = (cpu_count() or 2) + 4,
        prefetch: int = 0,
    ):
        super().__init__(mongo_connection, database, queue)
        if greenlet_threads <= 0:
            raise ValueError("greenlet_threads must be greater than 0")
        # Rename task_pool and _task_pool
        self.task_pool = Pool(greenlet_threads)
        # change to task pool
        self._task_pool = list()
        # add or * greenlet_threads for prefetch
        self.prefetch = greenlet_threads + prefetch
        # self.prefetch_pool = Pool(self.prefetch)
        # self._prefetch_pool = list()

    def wait(self):
        wait(self._task_pool)

    def worker(self, order: str = "fifo"):
        # add timer
        # add patterning matching
        # Add pause logic
        get_func = self.fifo
        while True:
            # Fix so it works with 0 prefetch for other worker types
            left = self.prefetch - len(self._local_queue)

            for _ in range(left):
                if func := get_func():
                    self._local_queue.append(func)

                elif func is None and self._local_queue == []:
                    self.watch()

                else:
                    break

            for func in self._local_queue:
                self._task_pool.append(self.task_pool.spawn(self.call_func, func))
                self._local_queue.remove(func)
                # Test to see if this loop blocks while waiting and if this is nessary
                # maybe put at top of for loop
                if self.task_pool.full():
                    break
