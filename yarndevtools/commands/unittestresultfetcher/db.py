import logging
from collections import UserDict
from typing import Dict

from marshmallow import Schema, fields, post_load, post_dump, pre_load, EXCLUDE

from yarndevtools.commands.unittestresultfetcher.model import JenkinsJobReport
from yarndevtools.common.common_model import JobBuildDataSchema, MONGO_COLLECTION_JENKINS_BUILD_DATA, JobBuildData
from yarndevtools.common.db import DBSerializable, Database, MongoDbConfig

LOG = logging.getLogger(__name__)


class JenkinsJobReportSchema(Schema):
    job_build_datas = fields.List(fields.Nested(JobBuildDataSchema()))
    all_failing_tests = fields.Dict(keys=fields.Str, values=fields.Int)
    total_no_of_builds = fields.Int()
    num_builds_per_config = fields.Int()

    @post_load
    def make_report(self, data, **kwargs):
        return JenkinsJobReport(**data)


class JenkinsJobReportsSchema(Schema):
    data = fields.Dict(keys=fields.Str, values=fields.Nested(JenkinsJobReportSchema()))

    @pre_load
    def add_data_prop(self, data, **kwargs):
        # Adding 'data' property to satisfy validation
        return {"data": data}

    @post_load
    def make_reports(self, data, **kwargs):
        return JenkinsJobReports(data["data"])

    @post_dump
    def post_dump(self, obj, **kwargs):
        if "data" in obj:
            return obj["data"]
        return obj


class JenkinsJobReports(DBSerializable, UserDict):
    def __init__(self, reports):
        super().__init__()
        self.data: Dict[str, JenkinsJobReport] = reports
        self._index = 0
        self._schema = JenkinsJobReportsSchema()

    def serialize(self):
        return self._schema.dump(self)

    def __getitem__(self, job_name):
        return self.data[job_name]

    def __setitem__(self, job_name, report):
        self.data[job_name] = report

    def __delitem__(self, job_name):
        del self.data[job_name]

    def __len__(self):
        return len(self.data)

    def print_email_status(self):
        LOG.info("Printing email send status for jobs and builds...")
        for job_name, jenkins_job_report in self.data.items():
            for job_url, job_build_data in jenkins_job_report._jobs_by_url.items():
                LOG.info("Job URL: %s, email sent: %s", job_url, job_build_data.mail_sent)


class UTResultFetcherDatabase(Database):
    MONGO_COLLECTION_JENKINS_REPORTS = "jenkins_reports"

    def __init__(self, conf: MongoDbConfig):
        super().__init__(conf)
        self._report_schema = JenkinsJobReportSchema()
        self._reports_schema = JenkinsJobReportsSchema()

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

    def save_reports(self, reports: JenkinsJobReports, log: bool = False):
        LOG.info("Saving Jenkins reports to Database")
        if log:
            LOG.debug("Final cached data object: %s", reports)
        super().save(reports, collection_name=UTResultFetcherDatabase.MONGO_COLLECTION_JENKINS_REPORTS)

    def load_reports(self) -> JenkinsJobReports:
        LOG.info("Trying to load Jenkins reports from Database")
        reports_dic: Dict[str, JenkinsJobReport] = super().find_one(
            collection_name=UTResultFetcherDatabase.MONGO_COLLECTION_JENKINS_REPORTS
        )
        if not reports_dic:
            reports_dic = {}
        if "_id" in reports_dic:
            del reports_dic["_id"]
        reports = self._reports_schema.load(reports_dic)
        reports.print_email_status()
        LOG.info("Loaded cached data from Database. Length of collection: %d", len(reports))
        return reports

    def find_and_validate_all_reports(self):
        result = []
        docs = self.load_reports()
        for doc in docs:
            dic = self._reports_schema.load(doc, unknown=EXCLUDE)
            result.append(JenkinsJobReport.deserialize(dic))
        return result
