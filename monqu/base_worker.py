from cloudpickle.cloudpickle_fast import loads, dumps
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from bson import Binary, ObjectId
from datetime import datetime
from typing import Union


class BaseWorker:
    def __init__(
        self, mongo_connection: str, database: str = "monqu", queue: str = "queue"
    ):
        self.client = MongoClient(mongo_connection)
        # multiqueue
        self.col = self.client[database][queue]
        self._local_queue = list()

    # change back to start_time as arg vs in dict
    def call_func(self, func: dict):
        try:
            if func.get("args") and func.get("kwargs"):
                returned = loads(func.get("func"))(
                    *loads(func.get("args")), **loads(func.get("kwargs"))
                )
            elif func.get("args"):
                returned = loads(func.get("func"))(*loads(func.get("args")))
            elif func.get("kwargs"):
                returned = loads(func.get("func"))(**loads(func.get("kwargs")))
            else:
                returned = loads(func.get("func"))()
            # maybe not include returned change to if returned:
            # if returned is not None:
            self.col.find_one_and_replace(
                {"_id": ObjectId(func.get("_id"))},
                {
                    "status": "completed",
                    "start_time": func.get("start_time"),
                    "end_time": datetime.now(),
                    "returned": Binary(dumps(returned)) if returned else None,
                },
            )

        except Exception as exc:
            # Check if it needs to be == pr <=
            if func.get("retries") == 0:
                self.col.find_one_and_update(
                    # see if Object id is needed
                    {"_id": ObjectId(func.get("_id"))},
                    {"$set": {"status": "failed", "error": repr(exc)}},
                )
            else:
                self.col.find_one_and_update(
                    {"_id": ObjectId(func.get("_id"))},
                    {
                        "$set": {"status": "retry", "error": repr(exc)},
                        "$inc": {"retries": -1},
                    },
                )

    def fifo(self):
        start_time = datetime.now()

        func = self.col.find_one_and_update(
            {"status": {"$in": [None, "retry"]}},
            {"$set": {"status": "running", "start_time": start_time}},
            sort=[("priority", -1), ("_id", 1)],
        )

        if func:
            func["start_time"] = start_time
            return func
        else:
            return None

    def _random_id(self) -> Union[str, None]:
        # Optimize to get func instead aggregate
        # add way to use proities with sample
        sample = list(
            self.col.aggregate(
                [
                    {"$match": {"status": {"$in": [None, "retry"]}}},
                    {"$sample": {"size": 1}},
                ]
            )
        )

        if sample:
            return sample[0].get("_id")
        else:
            return None

    def random(self):
        start_time = datetime.now()
        # add condtion to check if status is None or running depending on how find_one_and works
        func = self.col.find_one_and_update(
            # add check for status
            {"_id": self._random_id()},
            {"$set": {"status": "running", "start_time": start_time}},
        )

        if func:
            func["start_time"] = start_time
            return func
        else:
            return None

    def by_id(self, mongo_id: str):
        start_time = datetime.now()
        # Check staus to see if it has been complted maybe make this for failed tasks
        func = self.col.find_one_and_update(
            {"_id": mongo_id}, {"$set": {"status": "running", "start_time": start_time}}
        )

        # Change this to return error or something
        if func:
            func["start_time"] = start_time
            return func
        else:
            return None

    def watch(self):
        try:
            with self.col.watch(
                [{"$match": {"status": {"$in": [None, "retry"]}}}], batch_size=1
            ) as stream:
                for _ in stream:
                    break
        except PyMongoError:
            # The ChangeStream encountered an unrecoverable error or the
            # resume attempt failed to recreate the cursor.
            print("Pymongo Error")
