from cloudpickle.cloudpickle_fast import dumps
from pymongo import MongoClient
from bson import Binary
from typing import Union, Callable
from functools import wraps


# find replacemnt for object
# maybe switch queue to collection
class MonquServer:
    def __init__(self, mongo_connection: str, database: str = 'monqu', queue: str = 'queue'):
        self.client = MongoClient(mongo_connection)
        self.col = self.client[database][queue]
        self._tasks = []
        self._bulk_queue = []

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
        queue.insert_one(self._payload(func, args, kwargs, priority, retries))

    def bulk_enqueue(
            self,
            func: Callable,
            args: Union[tuple, list] = None,
            kwargs: dict = None,
            priority: int = 0,
            retries: int = 0
            # queue as option
    ):
        self._bulk_queue.append(self._payload(func, args, kwargs, priority, retries))

    def bulk_insert(self):
        self.col.insert_many(self._bulk_queue)

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
