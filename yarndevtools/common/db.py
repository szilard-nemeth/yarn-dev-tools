from abc import ABC, abstractmethod

import pymongo

import logging

LOG = logging.getLogger(__name__)


class DBSerializable(ABC):
    @abstractmethod
    def serialize(self):
        pass


class MongoDbConfig:
    def __init__(self, args, ensure_db_created=True):
        mongo_vars = {k.replace("mongo.", ""): v for k, v in vars(args).items() if k.startswith("mongo.")}
        if not mongo_vars:
            mongo_vars = {k.replace("mongo_", ""): v for k, v in vars(args).items() if k.startswith("mongo_")}

        self._validate_arg(args, mongo_vars, "hostname")
        self._validate_arg(args, mongo_vars, "port")
        self._validate_arg(args, mongo_vars, "user")
        self._validate_arg(args, mongo_vars, "password")
        self._validate_arg(args, mongo_vars, "db_name")

        self._dict = mongo_vars
        self._dict["ensure_db_created"] = ensure_db_created
        self._dict["force_create_db"] = mongo_vars.get("force_create_db", False)
        self._post_process()

    def _post_process(self):
        if self.ensure_db_created and self.force_create_db:
            LOG.warning("Setting 'ensure_db_created' to False as 'force_create_db' is enabled!")
            self._dict["ensure_db_created"] = False

    @staticmethod
    def _validate_arg(args, mongo_vars, name):
        if name not in mongo_vars or not mongo_vars[name]:
            raise ValueError("Mongo {} is not specified! Recognized args: {}".format(name, args))

    @property
    def hostname(self):
        return self._dict["hostname"]

    @property
    def port(self):
        return self._dict["port"]

    @property
    def user(self):
        return self._dict["user"]

    @property
    def password(self):
        return self._dict["password"]

    @property
    def db_name(self):
        return self._dict["db_name"]

    @property
    def force_create_db(self):
        return self._dict["force_create_db"]

    @property
    def ensure_db_created(self):
        return self._dict["ensure_db_created"]


class Database(ABC):
    def __init__(self, conf: MongoDbConfig):
        url = "mongodb://{user}:{password}@{hostname}:{port}/{db_name}?authSource=admin".format(
            user=conf.user, password=conf.password, hostname=conf.hostname, port=conf.port, db_name=conf.db_name
        )
        LOG.info("Using connection URL '%s' for mongodb", url.replace(conf.password, len(conf.password) * "*"))
        self._client = pymongo.MongoClient(url)

        if conf.ensure_db_created:
            dbnames = self._client.list_database_names()
            if conf.db_name not in dbnames:
                raise ValueError("DB with name '{}' does not exist!".format(conf.db_name))
        self._db = self._client[conf.db_name]

    def save(self, obj: DBSerializable, collection_name: str, id_field_name: str = None):
        serialized = obj.serialize()
        if id_field_name:
            if id_field_name not in serialized:
                raise ValueError("Serialized object '{}' has no field with name '{}'".format(serialized, id_field_name))
            # Manually add _id field for MongoDB.
            serialized["_id"] = serialized[id_field_name]
        LOG.debug("Serialized object to MongoDB:", serialized)

        # TODO bypass added to avoid: bson.errors.InvalidDocument: key 'Mawo-UT-hadoop-CDPD-7.x' must not contain '.'
        return self._db[collection_name].insert_one(serialized, bypass_document_validation=True)

    def find_by_id(self, id, collection_name: str):
        doc = self._db[collection_name].find_one({"_id": id})
        return doc

    def find_all(self, collection_name: str):
        collection = self._db[collection_name]
        return list(collection.find())

    def find_one(self, collection_name: str):
        collection = self._db[collection_name]
        res = collection.find_one()
        if not res:
            return None
        return res
