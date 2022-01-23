from SECRET import MONGO_URI
from gevent.monkey import patch_all; patch_all()
import gevent
from gevent.pool import Pool
from cloudpickle import register_pickle_by_value
from cloudpickle.cloudpickle_fast import dumps
import time
from pymongo import MongoClient
from bson import Binary
from typing import Union, Callable
from functools import wraps
# remove
import sniffio


# Non-gevent version
# make sure _tasks is correct and doesn't need to be Pool()
# find replacemnt for object
# maybe switch queue to collection
class MonquServer:
    def __init__(self, mongo_connection: str, database: str = 'monqu', queue: str = 'queue'):
        self.client = MongoClient(mongo_connection)
        self.col = self.client[database][queue]
        self._tasks = list()
        self._bulk_queue = list()

    def enqueue(
            self,
            func: Callable,
            args: Union[tuple, list] = None,
            kwargs: dict = None,
            priority: int = 0,
            retries: int = 0
            # queue as option
    ):
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

        self._tasks.append(gevent.spawn(self.col.insert_one, payload))

    def bulk_enqueue(
            self,
            func: Callable,
            args: Union[tuple, list] = None,
            kwargs: dict = None,
            priority: int = 0,
            retries: int = 0
            # queue as option
    ):
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

        self._bulk_queue.append(payload)

    def bulk_insert(self):
        self._tasks.append(gevent.spawn(self.col.insert_many, self._bulk_queue))

    def task(self, original_func: Callable = None, priority: int = 0, retries: int = 1):
        def wrapper(func):
            @wraps(func)
            def enqueue_wrapper(*args, **kwargs):
                self.enqueue(func, args, kwargs, priority, retries)
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


mq = MonquServer(MONGO_URI)
mq.wait()


# @mq.task
# def get(url: str):
#     try:
#         response = httpx.get(url, timeout=6)
#         if response.status_code == 200:
#             return response
#
#         elif (code := response.status_code) != 200:
#             print(f'ERROR STATUS CODE: {code}')
#
#     except httpx.RequestError or httpx.TimeoutException as exc:
#         print(f'An error occurred while requesting {exc.request.url!r}.')


# register_pickle_by_value(httpx)
#
# for i in range(10):
#     get('https://ip.selectrl.workers.dev')
