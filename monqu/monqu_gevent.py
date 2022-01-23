from gevent.monkey import patch_all; patch_all()
import gevent
from gevent import wait
from gevent.pool import Pool
from cloudpickle.cloudpickle_fast import dumps
from pymongo import MongoClient
from bson import Binary
from typing import Union, Callable
from functools import wraps
from base_worker import BaseWorker


# make sure _tasks is correct and doesn't need to be Pool()
# find replacemnt for object
# maybe switch queue to collection
class MonquServer:
    def __init__(self, mongo_connection: str, database: str = 'monqu', queue: str = 'queue'):
        self.client = MongoClient(mongo_connection)
        self.database = self.client[database]
        self.col = self.database[queue]
        self._tasks = list()
        self._bulk_queue = list()

    @staticmethod
    def _payload(
            func: Callable,
            args: Union[tuple, list],
            kwargs: dict,
            priority: int,
            retries: int
    ) -> dict:
        payload = {
            'status': None,
            'priority': priority,
            'retries': retries,
            'func': Binary(dumps(func))
        }
        if args:
            payload['args'] = Binary(dumps(args))

        if kwargs:
            payload['kwargs'] = Binary(dumps(kwargs))

        return payload

    def enqueue(
            self,
            func: Callable,
            args: Union[tuple, list] = None,
            kwargs: dict = None,
            queue: str = None,
            priority: int = 0,
            retries: int = 0
    ):
        queue = queue if queue else self.col
        self._tasks.append(gevent.spawn(queue.insert_one, self._payload(func, args, kwargs, priority, retries)))

    def bulk_enqueue(
            self,
            func: Callable,
            args: Union[tuple, list] = None,
            kwargs: dict = None,
            priority: int = 0,
            retries: int = 0
            # queue as option
    ) -> dict:
        self._bulk_queue.append(self._payload(func, args, kwargs, priority, retries))

    def bulk_insert(self):
        self._tasks.append(gevent.spawn(self.col.insert_many, self._bulk_queue))

    def task(self, original_func: Callable = None, queue: str = None, priority: int = 0, retries: int = 1):
        def wrapper(func):
            @wraps(func)
            def enqueue_wrapper(*args, **kwargs):
                self.enqueue(func, args, kwargs, queue, priority, retries)
            return enqueue_wrapper

        if original_func:
            return wrapper(original_func)
        return wrapper

    def bulk_task(self, original_func: Callable = None, priority: int = 0, retries: int = 1):
        def wrapper(func):
            @wraps(func)
            def bulk_enqueue_wrapper(*args, **kwargs):
                self.bulk_enqueue(func, args, kwargs, priority, retries)
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
            database: str = 'monqu',
            queue: str = 'queue',
            greenlet_threads: int = 10,
            prefetch: int = 0
    ):
        super().__init__(mongo_connection, database, queue)
        self.task_pool = Pool(greenlet_threads)
        # change to task pool
        self._task_pool = list()
        # add or * greenlet_threads for prefetch
        self.prefetch = greenlet_threads + prefetch
        # self.prefetch_pool = Pool(self.prefetch)
        # self._prefetch_pool = list()

    def wait(self):
        wait(self._task_pool)

    def worker(self, order: str = 'fifo'):
        # add timer
        # add patterning matching
        # Add pause logic
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
                self._task_pool.append(self.task_pool.spawn(self.call_func, func))
                self._local_queue.remove(func)
                # Test to see if this loop blocks while waiting and if this is nessary
                # maybe put at top of for loop
                if self.task_pool.full():
                    break
