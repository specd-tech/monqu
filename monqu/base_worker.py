from cloudpickle.cloudpickle_fast import loads, dumps
from pymongo import MongoClient, ReplaceOne
from pymongo.errors import PyMongoError
from bson import Binary, ObjectId
from datetime import datetime
from abc import ABC

"""TODO
convert find_and where not needed
for doc strings should it be pymongo or MongoDB
"""


# when finished make all vars private
class BaseWorker(ABC):
    def __init__(
        self,
        mongo_connection: str,
        database: str = "monqu",
        queue: str = "queue",
        prefetch: int = 0,
    ):
        self.client = MongoClient(mongo_connection)
        self._is_replica_set = (
            True if self.client.topology_description.topology_type else False
        )
        # multiqueue
        self.col = self.client[database][queue]
        self._local_queue = []
        self.prefetch = prefetch
        # Not implemented
        self._pause = False
        self._shutdown = False

    def call_func(self, func: dict, bulk: bool = False) -> ReplaceOne | None:
        """Deserializes and calls function.

        Args:
            func: Dictionary containing a serialized function and auxiliary/additional information.
            bulk: Boolean that controls how the called function's result is handled.

        Returns:
            If bulk is False there is no return value. If bulk is True the called function's result is serialized and
            returned in a pymongo ReplaceOne object.

        """
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
            # Does it need to be find_one_and_replace
            # ObjectId(func.get("_id"))}?

            # ? Prepares return dict
            _id = {"_id": ObjectId(func.get("_id"))}
            return_func = {
                "status": "completed",
                "start_time": func.get("start_time"),
                "end_time": datetime.now(),
            }
            # If the function returned any value, this serializes it and adds it to the return payload
            if returned is not None:
                return_func["returned"] = Binary(dumps(returned))

            if bulk is False:
                self.col.find_one_and_replace(_id, return_func)
            if bulk is True:
                return ReplaceOne(_id, return_func)

        except Exception as exc:
            # Make sure retries can't be below zero
            if func.get("retries") == 0:
                self.col.find_one_and_update(
                    # see if Object id is needed
                    {"_id": ObjectId(func.get("_id"))},
                    {"$set": {"status": "failed", "exception": repr(exc)}},
                )
            else:
                self.col.find_one_and_update(
                    {"_id": ObjectId(func.get("_id"))},
                    {
                        "$set": {"status": "retry", "exception": repr(exc)},
                        "$inc": {"retries": -1},
                    },
                )

    # remove bulk and keep it funcs vs func
    def bulk_call_funcs(self, funcs: list[dict]) -> None:
        """Calls multiple functions and inserts the results to the queue.

        Passes funcs to call_func with bulk flag set to true so the functions' results are returned. The returned
        dictionaries are then inserted to the queue using MongoDB's bulk_write.

        Args:
            funcs: List of dictionaries contain serialized functions.

        """
        return_payload = [self.call_func(func, bulk=True) for func in funcs]
        self.col.bulk_write(return_payload)

    # Rename
    def _set_up_funcs(self, cursor, session) -> list[dict] | None:
        """

        Args:
            cursor: MongoDB cursor
            session: MongoDB session

        Returns:

        """
        start_time = datetime.now()
        # Converts funcs in cursor to dictionary and adds start_time. This needs to be done before None check.
        funcs = [dict(func, start_time=start_time) for func in cursor]

        if funcs is not None:
            self.col.update_many(
                {"_id": {"$in": [func.get("_id") for func in funcs]}},
                {"$set": {"status": "running", "start_time": start_time}},
                session=session,
            )
            return funcs
        else:
            return None

    def bulk_fifo(self) -> list[dict] | None:
        """

        Returns:

        """
        with self.client.start_session() as session:
            with session.start_transaction():
                cursor = self.col.find(
                    {"status": {"$in": [None, "retry"]}},
                    sort=[("priority", -1), ("_id", 1)],
                    session=session,
                ).limit(self.prefetch)

                return self._set_up_funcs(cursor=cursor, session=session)

    def bulk_lifo(self) -> list[dict] | None:
        """

        Returns:

        """
        with self.client.start_session() as session:
            with session.start_transaction():
                cursor = self.col.find(
                    {"status": {"$in": [None, "retry"]}},
                    sort=[("priority", -1), ("_id", -1)],
                    session=session,
                ).limit(self.prefetch)

                return self._set_up_funcs(cursor=cursor, session=session)

    def bulk_random(self) -> list[dict] | None:
        """

        Returns:

        """
        with self.client.start_session() as session:
            with session.start_transaction():
                cursor = self.col.aggregate(
                    [
                        {"$match": {"status": {"$in": [None, "retry"]}}},
                        {"$sample": {"size": self.prefetch}},
                    ],
                    session=session,
                )

                return self._set_up_funcs(cursor=cursor, session=session)

    def fifo(self) -> dict | None:
        """First in first out

        Finds and returns the first/oldest and highest priority task in the queue.

        Returns:
            Dictionary with serialized function if there is any uncompleted tasks in the queue. If there are no tasks
            then None is returned.

        """
        start_time = datetime.now()

        func = self.col.find_one_and_update(
            {"status": {"$in": [None, "retry"]}},
            {"$set": {"status": "running", "start_time": start_time}},
            sort=[("priority", -1), ("_id", 1)],
        )

        if func is not None:
            func["start_time"] = start_time
            return func
        else:
            return None

    def lifo(self) -> dict | None:
        """Last in first out

        Finds and returns the last/newest and highest priority task in the queue.

        Returns:
            Dictionary with serialized function if there is any uncompleted tasks in the queue. If there are no tasks
            then None is returned.

        """
        start_time = datetime.now()

        func = self.col.find_one_and_update(
            {"status": {"$in": [None, "retry"]}},
            {"$set": {"status": "running", "start_time": start_time}},
            sort=[("priority", -1), ("_id", -1)],
        )

        if func is not None:
            func["start_time"] = start_time
            return func
        else:
            return None

    def _random_id(self) -> str | None:
        sample = list(
            self.col.aggregate(
                [
                    {"$match": {"status": {"$in": [None, "retry"]}}},
                    # Change to prefetch amount?
                    {"$sample": {"size": 1}},
                ]
            )
        )

        if sample is not None:
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

        if func is not None:
            func["start_time"] = start_time
            return func
        else:
            return None

    # change to call func if exist else return None
    def by_id(self, mongodb_id: str) -> dict | None:
        """

        Args:
            mongodb_id:

        Returns:

        """
        start_time = datetime.now()
        # Check status to see if it has been completed maybe make this for failed tasks
        func = self.col.find_one_and_update(
            {"_id": mongodb_id}, {"$set": {"status": "running", "start_time": start_time}}
        )

        # Change this to return error or something
        if func is not None:
            func["start_time"] = start_time
            return func
        else:
            return None

    def watch(self):
        """Blocks until there is a new task add to the queue."""
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
