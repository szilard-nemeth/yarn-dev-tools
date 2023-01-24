import datetime
import logging
from collections import UserDict
from dataclasses import dataclass
from typing import Dict, List, Set

from marshmallow import Schema, fields, EXCLUDE, post_load
from pythoncommons.date_utils import DateUtils

from yarndevtools.commands.unittestresultfetcher.model import JenkinsJobResult
from yarndevtools.common.common_model import JobBuildData, DBSerializable
from yarndevtools.common.db import Database, MongoDbConfig, JenkinsBuildDatabase

LOG = logging.getLogger(__name__)


class MailSendStateSchema(Schema):
    sent = fields.Boolean()
    sent_date = fields.DateTime(allow_none=True)

    @post_load
    def make_obj(self, data, **kwargs):
        return MailSendState(**data)


class MailSendStateForJobSchema(Schema):
    build_url = fields.Str(required=True)
    job_name = fields.Str(required=True)
    recipients = fields.Dict(keys=fields.Str, values=fields.Nested(MailSendStateSchema()))

    @post_load
    def make_obj(self, data, **kwargs):
        return MailSendStateForJob(**data)


@dataclass
class MailSendState:
    sent: bool
    sent_date: datetime.datetime or None

    def reset(self):
        self.sent = False
        self.sent_date = None


@dataclass
class MailSendStateForJob(DBSerializable):
    build_url: str
    job_name: str
    # TODO yarndevtoolsv2: use defaultdict
    recipients: Dict[str, MailSendState]

    def __post_init__(self):
        self._schema = MailSendStateForJobSchema()

    def serialize(self):
        return self._schema.dump(self)

    def add_recipient(self, recipient, sent, sent_date):
        if recipient not in self.recipients or not self.recipients[recipient].sent:
            self.recipients[recipient] = MailSendState(sent, sent_date)

    def reset(self):
        for mail_send_state in self.recipients.values():
            mail_send_state.reset()


class MailSendStateTracker(UserDict):
    def __init__(self, objs: List[MailSendStateForJob]):
        super().__init__()
        # TODO yarndevtoolsv2: use defaultdict
        self.data: Dict[str, MailSendStateForJob] = {}  # Key: Job URL
        for obj in objs:
            # We can assume that URL is unique
            self.data[obj.build_url] = obj
        self._index = 0

    def __getitem__(self, job_url):
        return self.data[job_url]

    def __setitem__(self, job_url, mail_send_state):
        self.data[job_url] = mail_send_state

    def __delitem__(self, job_url):
        del self.data[job_url]

    def __len__(self):
        return len(self.data)

    def is_mail_sent(self, job_url, recipients) -> Set[str]:
        """

        :param job_url:
        :param recipients:
        :return: Set of recipients that the email is not sent for this job
        """
        if job_url not in self.data:
            return recipients

        sent_to_recipients = set(self.data[job_url].recipients)
        wanted_recipients = set(recipients)
        missing_recipients = wanted_recipients.difference(sent_to_recipients)

        if missing_recipients:
            return missing_recipients
        return set()

    def get_val(self, job_url, job_name):
        # TODO yarndevtoolsv2: Is there a cleaner way to do this?
        if job_url not in self.data:
            self.data[job_url] = MailSendStateForJob(job_url, job_name, {})
        return self.data[job_url]

    def add_build_with_mail_data(self, job_url, job_name, recipient, sent, sent_date):
        if job_url not in self.data:
            self.data[job_url] = MailSendStateForJob(job_url, job_name, {recipient: MailSendState(sent, sent_date)})
        else:
            mail_sent_to_recipients = self.data[job_url]
            mail_sent_to_recipients.add_recipient(recipient, sent, sent_date)

    def reset_for_job(self, job_name):
        for mail_send_state_for_job in self.data.values():
            if mail_send_state_for_job.job_name == job_name:
                mail_send_state_for_job.reset()

    def mark_sent(self, job_url, job_name, recipients):
        dt = DateUtils.now()
        if job_url not in self.data:
            self.data[job_url] = MailSendStateForJob(
                job_url, job_name, {r: MailSendState(True, dt) for r in recipients}
            )
        else:
            for r in recipients:
                self.data[job_url].add_recipient(r, True, dt)

    def print(self):
        # TODO yarndevtoolsv2 prettyprint + invoke
        pass

    # def print_email_status(self):
    #     LOG.info("Printing email send status for jobs and builds...")
    #     for job_name, job_result in self.data.items():
    #         for job_url, job_build_data in job_result._builds_by_url.items():
    #             LOG.info("Job URL: %s, email sent: %s", job_url, job_build_data.mail_sent)


class JenkinsJobResults(UserDict):
    def __init__(self, job_results):
        super().__init__()
        self.data: Dict[str, JenkinsJobResult] = job_results  # key: Jenkins job name
        self._index = 0

    def __getitem__(self, job_name):
        return self.data[job_name]

    def __setitem__(self, job_name, job_result):
        self.data[job_name] = job_result

    def __delitem__(self, job_name):
        del self.data[job_name]

    def __len__(self):
        return len(self.data)

    def get_by_job_and_url(self, job_name, url):
        return self.data[job_name]._builds_by_url[url]


class UTResultFetcherDatabase(Database):
    MONGO_COLLECTION_EMAIL_SEND_STATE = "email_send_state"

    def __init__(self, conf: MongoDbConfig):
        super().__init__(conf)
        self._jenkins_build_data_db = JenkinsBuildDatabase(conf)
        self._mail_send_state_for_job_schema = MailSendStateForJobSchema()

    def has_build_data(self, build_url):
        return self._jenkins_build_data_db.has_build_data(build_url)

    def save_build_data(self, build_data: JobBuildData):
        self._jenkins_build_data_db.save_build_data(build_data)

    def save_job_results(self, job_results: JenkinsJobResults, log: bool = False):
        LOG.info("Saving Jenkins job results to Database")
        if log:
            LOG.debug("Final job results object: %s", job_results)

        job_build_datas = []
        for job_result in job_results.data.values():
            for build in job_result.builds:
                job_build_datas.append(build)
        self._jenkins_build_data_db.save_all_build_data(job_build_datas)

    def load_job_results(self) -> JenkinsJobResults:
        LOG.info("Trying to load Jenkins job results from Database")
        job_build_data_objs = self._jenkins_build_data_db.find_and_validate_all_build_data()

        job_results_dict = {}
        for build_data in job_build_data_objs:
            if build_data.job_name not in job_results_dict:
                job_results_dict[build_data.job_name] = JenkinsJobResult.create_empty()
            job_results_dict[build_data.job_name].add_build(build_data)

        for job_result in job_results_dict.values():
            job_result.finalize()

        job_results = JenkinsJobResults(job_results_dict)
        LOG.info("Loaded Jenkins job results from Database. Length of collection: %d", len(job_results))
        return job_results

    def load_email_send_state(self) -> List[MailSendStateForJob]:
        LOG.info("Loading email send state")
        docs = self.find_all(collection_name=UTResultFetcherDatabase.MONGO_COLLECTION_EMAIL_SEND_STATE)
        objs: List[MailSendStateForJob] = []
        for doc in docs:
            obj: MailSendStateForJob = self._mail_send_state_for_job_schema.load(doc, unknown=EXCLUDE)
            objs.append(obj)
        return objs

    def save_email_send_state(self, obj: MailSendStateForJob):
        LOG.debug("Saving email send state for: %s", obj.build_url)
        if not obj:
            return
        self.save(
            obj,
            replace=True,
            collection_name=UTResultFetcherDatabase.MONGO_COLLECTION_EMAIL_SEND_STATE,
            id_field_name="build_url",
        )

    def reset_email_send_state(self, tracker: MailSendStateTracker, job_names: List[str]):
        LOG.info("Resetting email send state on the following jobs: %s", job_names)
        for job_name in job_names:
            tracker.reset_for_job(job_name)

        docs = [d.serialize() for d in tracker.data.values()]
        # TODO Specify id ?
        self.save_many(
            docs, collection_name=UTResultFetcherDatabase.MONGO_COLLECTION_EMAIL_SEND_STATE, id_field_name="build_url"
        )
