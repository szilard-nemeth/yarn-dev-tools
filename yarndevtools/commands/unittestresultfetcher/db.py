import logging
from collections import UserDict
from typing import Dict

from marshmallow import Schema, fields, post_load, post_dump, pre_load, EXCLUDE

from yarndevtools.commands.unittestresultfetcher.model import JenkinsJobResult
from yarndevtools.common.common_model import JobBuildDataSchema, MONGO_COLLECTION_JENKINS_BUILD_DATA, JobBuildData
from yarndevtools.common.db import DBSerializable, Database, MongoDbConfig

LOG = logging.getLogger(__name__)


class JenkinsJobResultSchema(Schema):
    job_build_datas = fields.List(fields.Nested(JobBuildDataSchema()))
    all_failing_tests = fields.Dict(keys=fields.Str, values=fields.Int)
    total_no_of_builds = fields.Int()
    num_builds_per_config = fields.Int()

    @post_load
    def make_job_result_obj(self, data, **kwargs):
        return JenkinsJobResult(**data)


class JenkinsJobResultsSchema(Schema):
    data = fields.Dict(keys=fields.Str, values=fields.Nested(JenkinsJobResultSchema()))

    @pre_load
    def add_data_prop(self, data, **kwargs):
        # Adding 'data' property to satisfy validation
        return {"data": data}

    @post_load
    def make_job_results_obj(self, data, **kwargs):
        return JenkinsJobResults(data["data"])

    @post_dump
    def post_dump(self, obj, **kwargs):
        if "data" in obj:
            return obj["data"]
        return obj


class JenkinsJobResults(DBSerializable, UserDict):
    def __init__(self, job_results):
        super().__init__()
        self.data: Dict[str, JenkinsJobResult] = job_results
        self._index = 0
        self._schema = JenkinsJobResultsSchema()

    def serialize(self):
        return self._schema.dump(self)

    def __getitem__(self, job_name):
        return self.data[job_name]

    def __setitem__(self, job_name, job_result):
        self.data[job_name] = job_result

    def __delitem__(self, job_name):
        del self.data[job_name]

    def __len__(self):
        return len(self.data)

    def print_email_status(self):
        LOG.info("Printing email send status for jobs and builds...")
        for job_name, job_result in self.data.items():
            for job_url, job_build_data in job_result._jobs_by_url.items():
                LOG.info("Job URL: %s, email sent: %s", job_url, job_build_data.mail_sent)


class UTResultFetcherDatabase(Database):
    MONGO_COLLECTION_JENKINS_JOB_RESULTS = "jenkins_job_results"

    def __init__(self, conf: MongoDbConfig):
        super().__init__(conf)
        self._job_result_schema = JenkinsJobResultSchema()
        self._job_results_schema = JenkinsJobResultsSchema()
        self._coll = UTResultFetcherDatabase.MONGO_COLLECTION_JENKINS_JOB_RESULTS

    def has_build_data(self, build_url):
        doc = super().find_by_id(build_url, collection_name=MONGO_COLLECTION_JENKINS_BUILD_DATA)
        return True if doc else False

    def save_build_data(self, build_data: JobBuildData):
        # TODO trace logging
        # LOG.debug("Saving build data to Database: %s", build_data)
        doc = super().find_by_id(build_data.build_url, collection_name=MONGO_COLLECTION_JENKINS_BUILD_DATA)
        # TODO yarndevtoolsv2 Overwrite of saved fields won't happen here (e.g. mail sent = True)
        if doc:
            return doc
        return super().save(build_data, collection_name=MONGO_COLLECTION_JENKINS_BUILD_DATA, id_field_name="build_url")

    def save_job_results(self, job_results: JenkinsJobResults, log: bool = False):
        LOG.info("Saving Jenkins job results to Database")
        if log:
            LOG.debug("Final job results object: %s", job_results)
        super().save(job_results, collection_name=self._coll)

    def load_job_results(self) -> JenkinsJobResults:
        LOG.info("Trying to load Jenkins job results from Database")
        count = super().count(self._coll)
        if count > 1:
            raise ValueError("Expected count of collection '{}' is 1. Actual count: {}".format(self._coll, count))

        job_results_dict: Dict[str, JenkinsJobResult] = super().find_one(collection_name=self._coll)
        if not job_results_dict:
            job_results_dict = {}
        if "_id" in job_results_dict:
            del job_results_dict["_id"]
        job_results = self._job_results_schema.load(job_results_dict)
        job_results.print_email_status()
        LOG.info("Loaded Jenkins job results from Database. " "Length of collection: %d", len(job_results))
        return job_results

    def find_and_validate_all_job_results(self):
        all_job_results = []
        docs = self.load_job_results()
        for doc in docs:
            dic = self._job_results_schema.load(doc, unknown=EXCLUDE)
            all_job_results.append(JenkinsJobResult.deserialize(dic))
        return all_job_results
