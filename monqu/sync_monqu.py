from cloudpickle.cloudpickle_fast import dumps
from pymongo import MongoClient
from bson import Binary
from typing import Union, Callable
from functools import wraps
from collections import defaultdict

"""
TODO:
Check if value error shows properly in different functions
test bulk_insert with auto_insert for performance loss
add module check for gevent
Check error conventions
"""


# flip ret and proiety
class MonquServer:
    def __init__(
        self,
        mongo_connection: str,
        database: str = "monqu",
        queue: str = "queue",
        auto_insert: int = None,
    ):
        self.client = MongoClient(mongo_connection)
        self.database = self.client[database]
        self.col = self.database[queue]
        self.queue = queue
        if auto_insert is not None and auto_insert <= 1:
            raise ValueError("auto_insert must be greater then 1")
        else:
            self.auto_insert = auto_insert
            self.bulk_count = 1
        self._bulk_queue = defaultdict(list)

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
        if func is not None and not callable(func):
            # Correct wording
            raise TypeError("func must be callable")

        queue = queue if queue else self.col
        queue.insert_one(self._payload(func, args, kwargs, priority, retries))

    def bulk_enqueue(
        self,
        func: Callable,
        args: Union[tuple, list] = None,
        kwargs: dict = None,
        queue: str = None,
        priority: int = 0,
        retries: int = 0,
    ):
        if func is not None and not callable(func):
            raise TypeError("func must be callable")

        queue = queue if queue else self.queue
        self._bulk_queue[queue] += [
            self._payload(func, args, kwargs, priority, retries)
        ]

        if self.auto_insert is None:
            pass
        elif self.bulk_count == self.auto_insert:
            self.bulk_insert()
            self.bulk_count = 1
        else:
            self.bulk_count += 1

    def bulk_insert(self):
        for queue, tasks in self._bulk_queue.items():
            self.database[queue].insert_many(tasks)

        self._bulk_queue.clear()

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
