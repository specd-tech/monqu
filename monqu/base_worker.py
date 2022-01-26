from cloudpickle.cloudpickle_fast import loads, dumps
from pymongo import MongoClient, ReplaceOne
from pymongo.errors import PyMongoError
from bson import Binary, ObjectId
from datetime import datetime


class BaseWorker:
    def __init__(
        self,
        mongo_connection: str,
        database: str = "monqu",
        queue: str = "queue",
        prefetch: int = 0,
    ):
        self.client = MongoClient(mongo_connection)
        # multiqueue
        self.col = self.client[database][queue]
        self._local_queue = []
        self.prefetch = prefetch

    def call_func(self, func: dict, bulk: bool = False) -> ReplaceOne | None:
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
            # Does it need to be find_one_and_replace
            if bulk is False:
                self.col.find_one_and_replace(
                    {"_id": ObjectId(func.get("_id"))},
                    {
                        "status": "completed",
                        "start_time": func.get("start_time"),
                        "end_time": datetime.now(),
                        # Check if returned is None so NoneType is not converted to binary
                        "returned": Binary(dumps(returned)) if returned else None,
                    },
                )
            if bulk is True:
                return ReplaceOne(
                    {"_id": ObjectId(func.get("_id"))},
                    {
                        "status": "completed",
                        "start_time": func.get("start_time"),
                        "end_time": datetime.now(),
                        # Check if returned is None so NoneType is not converted to binary
                        "returned": Binary(dumps(returned)) if returned else None,
                    },
                )

        except Exception as exc:
            # Make sure retries can't be below zero
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

    # remove bulk and keep it funcs vs func
    def bulk_call_funcs(self, funcs: list[dict]):
        return_payload = [self.call_func(func, bulk=True) for func in funcs]
        self.col.bulk_write(return_payload)

    def fifo(self) -> dict | None:
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

    # Rename
    def _start_funcs(self, cursor, session) -> list[dict] | None:
        start_time = datetime.now()
        funcs = [dict(func, start_time=start_time) for func in cursor]

        if funcs:
            self.col.update_many(
                {"_id": {"$in": [func.get("_id") for func in funcs]}},
                {"$set": {"status": "running", "start_time": start_time}},
                session=session,
            )
            return funcs
        else:
            return None

    # Check if type hinting is correct
    def bulk_fifo(self) -> list[dict] | None:
        with self.client.start_session() as session:
            with session.start_transaction():
                cursor = self.col.find(
                    {"status": {"$in": [None, "retry"]}},
                    sort=[("priority", -1), ("_id", 1)],
                    session=session,
                ).limit(self.prefetch)

                return self._start_funcs(cursor=cursor, session=session)

    def _random_id(self) -> str | None:
        # Optimize to get func instead aggregate
        # add way to use proities with sample
        sample = list(
            self.col.aggregate(
                [
                    {"$match": {"status": {"$in": [None, "retry"]}}},
                    # Change to prefetch amount?
                    {"$sample": {"size": 1}},
                ]
            )
        )

        if sample:
            return sample[0].get("_id")
        else:
            return None

    def random(self) -> dict | None:
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

    def bulk_random(self):
        with self.client.start_session() as session:
            with session.start_transaction():

                cursor = self.col.aggregate(
                    [
                        {"$match": {"status": {"$in": [None, "retry"]}}},
                        # priority for random? sort=[("priority", -1), ("_id", 1)],
                        {"$sample": {"size": self.prefetch}},
                    ],
                    session=session,
                )

                return self._start_funcs(cursor=cursor, session=session)

    def by_id(self, mongo_id: str) -> dict | None:
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
