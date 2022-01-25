from cloudpickle.cloudpickle_fast import dumps
from pymongo import MongoClient
from bson import Binary
from typing import Union, Callable
from functools import wraps
from collections import defaultdict

"""
TODO:
Check if value error shows properly in different functions
"""


# flip ret and proiety
class MonquServer:
    def __init__(
        self, mongo_connection: str, database: str = "monqu", queue: str = "queue"
    ):
        self.client = MongoClient(mongo_connection)
        self.database = self.client[database]
        self.col = self.database[queue]
        self.queue = queue
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
            # Correct wording
            raise TypeError("func must be callable")
        queue = queue if queue else self.queue
        self._bulk_queue[queue] += [
            self._payload(func, args, kwargs, priority, retries)
        ]

    def bulk_insert(self):
        # Change terms?
        for queue, tasks in self._bulk_queue.items():
            self.database[queue].insert_many(tasks)

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
