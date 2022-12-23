from abc import ABC, abstractmethod

import pymongo

import logging

LOG = logging.getLogger(__name__)


class DBSerializable(ABC):
    @abstractmethod
    def serialize(self):
        pass


class MongoDbConfig:
    def __init__(self, args):
        self.hostname = args.hostname
        self.port = args.port
        self.user = args.user
        self.password = args.password
        self.db_name = args.db_name

        if not self.hostname:
            raise ValueError("Mongo hostname is not specified!")
        if not self.port:
            raise ValueError("Mongo port is not specified!")
        if not self.user:
            raise ValueError("Mongo user is not specified!")
        if not self.password:
            raise ValueError("Mongo password is not specified!")
        if not self.db_name:
            raise ValueError("Mongo DB name is not specified!")


class Database(ABC):
    def __init__(self, conf: MongoDbConfig):
        url = "mongodb://{user}:{password}@{hostname}:{port}/{db_name}?authSource=admin".format(
            user=conf.user, password=conf.password, hostname=conf.hostname, port=conf.port, db_name=conf.db_name
        )
        LOG.info("Using connection URL '%s' for mongodb", url)
        self._client = pymongo.MongoClient(url)
        self._db = self._client[conf.db_name]

    def save(self, obj: DBSerializable, collection: str, id_field_name: str = None):
        serialized = obj.serialize()

        if id_field_name:
            if not hasattr(serialized, id_field_name):
                raise ValueError("Serialized object '{}' has no field with name '{}'".format(serialized, id_field_name))
            # Manually add _id field for MongoDB.
            serialized["_id"] = serialized[id_field_name]
        LOG.debug("Serialized object to MongoDB:", serialized)

        return self._db[collection].insert_one(serialized)
