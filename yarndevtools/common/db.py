from abc import ABC
from typing import List, Any

import pymongo

import logging

from marshmallow import EXCLUDE

from yarndevtools.common.common_model import JobBuildDataSchema, JobBuildData, DBSerializable

LOG = logging.getLogger(__name__)


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

    def save(self, obj: DBSerializable, collection_name: str, id_field_name: str = None, replace=False, log_obj=False):
        serialized = self._serialize_obj(obj, id_field_name=id_field_name, log_obj=log_obj)
        # TODO bypass added to avoid: bson.errors.InvalidDocument: key 'Mawo-UT-hadoop-CDPD-7.x' must not contain '.'
        if replace:
            if not id_field_name:
                raise ValueError("'id_field_name' must be specified for replace!")
            real_id = serialized[id_field_name]

            if self.find_by_id(real_id, collection_name=collection_name):
                return self._db[collection_name].replace_one({"_id": real_id}, serialized)

        return self._db[collection_name].insert_one(serialized)

    @staticmethod
    def _serialize_obj(obj, id_field_name=None, log_obj=False):
        serialized = obj.serialize()
        if id_field_name:
            if id_field_name not in serialized:
                raise ValueError("Serialized object '{}' has no field with name '{}'".format(serialized, id_field_name))
            # Manually add _id field for MongoDB.
            serialized["_id"] = serialized[id_field_name]
        if log_obj:
            LOG.debug("Serialized object to MongoDB: %s", serialized)
        return serialized

    def save_many(
        self, obj_list: List[Any], collection_name: str, id_field_name: str = None, log_obj=False, force_mode=False
    ):
        if not obj_list:
            raise ValueError("Cannot save a non-empty list to DB!")
        # TODO Implement force_mode
        filtered_objs = self._filter_objs_by_ids(collection_name, id_field_name, obj_list)

        if not filtered_objs:
            LOG.warning(
                "Received %d objects, not persisting any of them as they were all persisted into the DB.", len(obj_list)
            )
            return

        serialized_objs = []
        for obj in filtered_objs:
            serialized = self._serialize_obj(obj, id_field_name=id_field_name, log_obj=log_obj)
            serialized_objs.append(serialized)
        res = self._db[collection_name].insert_many(serialized_objs)
        LOG.debug("Inserted %d documents into collection: %s", len(res.inserted_ids), collection_name)

    def _filter_objs_by_ids(self, collection_name, id_field_name, obj_list):
        if not id_field_name:
            raise ValueError("id_field_name is not specified, ID filtering cannot work without it!")
        ids = set(self._get_all_ids(collection_name))
        LOG.debug("Found %d documents in collection %s", len(ids), collection_name)
        filtered_objs = []
        for obj in obj_list:
            id = getattr(obj, id_field_name)
            if id not in ids:
                filtered_objs.append(obj)

        LOG.debug("Found %d objects with unknown IDs", len(filtered_objs))
        return filtered_objs

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

    def count(self, collection_name: str):
        return self._db[collection_name].count_documents({})

    def _get_all_ids(self, collection_name: str):
        return self._db[collection_name].find().distinct("_id")


class JenkinsBuildDatabase(Database):
    MONGO_COLLECTION_JENKINS_BUILD_DATA = "jenkins_build_data"

    def __init__(self, conf: MongoDbConfig):
        super().__init__(conf)
        self._build_data_schema = JobBuildDataSchema()
        self._coll = JenkinsBuildDatabase.MONGO_COLLECTION_JENKINS_BUILD_DATA

    def has_build_data(self, build_url):
        doc = super().find_by_id(build_url, collection_name=self._coll)
        return True if doc else False

    def find_all_build_data(self):
        return super().find_all(collection_name=self._coll)

    def find_and_validate_all_build_data(self) -> List[JobBuildData]:
        result = []
        docs = self.find_all_build_data()
        for doc in docs:
            job_build_data = self._build_data_schema.load(doc, unknown=EXCLUDE)
            result.append(job_build_data)
        return result

    def save_all_build_data(self, build_data_list: List[JobBuildData]):
        super().save_many(build_data_list, collection_name=self._coll, id_field_name="build_url")

    def save_build_data(self, build_data: JobBuildData):
        # TODO trace logging
        # LOG.debug("Saving build data to Database: %s", build_data)
        doc = super().find_by_id(build_data.build_url, collection_name=self._coll)
        # TODO yarndevtoolsv2 Overwrite of saved fields won't happen here (e.g. mail sent = True)
        if doc:
            return doc
        return super().save(build_data, collection_name=self._coll, id_field_name="build_url")
