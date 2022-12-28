import datetime
from pprint import pformat
from typing import List, Dict, Set, Tuple

from marshmallow import fields, Schema, EXCLUDE
from pythoncommons.date_utils import DateUtils

from yarndevtools.commands.unittestresultaggregator.common.model import (
    EmailContentProcessor,
    EmailMetaData,
    FailedBuildAbs,
)
from yarndevtools.common.common_model import JobBuildData, MONGO_COLLECTION_JENKINS_REPORTS, JobBuildDataSchema
from yarndevtools.common.db import MongoDbConfig, Database, DBSerializable
import logging

MONGO_COLLECTION_EMAIL_CONTENT = "email_data"
LOG = logging.getLogger(__name__)


class EmailContentSchema(Schema):
    msg_id = fields.Str(required=True)
    thread_id = fields.Str(required=True)
    date = fields.DateTime(required=True)
    subject = fields.Str(required=True)
    build_url = fields.Str(required=True)
    job_name = fields.Str(required=True)
    build_number = fields.Str(required=True)
    lines = fields.List(fields.Str)


class EmailContent(DBSerializable):
    def __init__(self, msg_id, thread_id, date, subject, build_url, job_name, build_number, lines):
        self.msg_id = msg_id
        self.thread_id = thread_id
        self.date: datetime.datetime = date
        self.subject = subject
        self.build_url = build_url
        self.job_name = job_name
        self.build_number = build_number
        self.lines = lines

    @staticmethod
    def from_message(email_meta: EmailMetaData, lines: List[str]):
        return EmailContent(
            email_meta.message_id,
            email_meta.thread_id,
            email_meta.date,
            email_meta.subject,
            email_meta.build_url,
            email_meta.job_name,
            email_meta.build_number,
            lines,
        )

    def serialize(self):
        schema = EmailContentSchema()
        output = schema.dump(self)
        return output


class UTResultAggregatorDatabase(Database):
    def __init__(self, conf: MongoDbConfig):
        super().__init__(conf)
        self._email_content_schema = EmailContentSchema()
        self._build_data_schema = JobBuildDataSchema()

    def find_email_content(self, id: str):
        return super().find_by_id(id, collection_name=MONGO_COLLECTION_EMAIL_CONTENT)

    def find_and_validate_email_content(self, id: str):
        doc = self.find_email_content(id)
        if not doc:
            return None

        dic = self._email_content_schema.load(doc, unknown=EXCLUDE)
        return EmailContent(**dic)

    def find_and_validate_all_email_content(self):
        result = []
        docs = self.find_all_email_content()
        for doc in docs:
            dic = self._email_content_schema.load(doc, unknown=EXCLUDE)
            result.append(EmailContent(**dic))
        return result

    def find_all_email_content(self):
        return super().find_all(collection_name=MONGO_COLLECTION_EMAIL_CONTENT)

    def save_email_content(self, email_content: EmailContent):
        return super().save(email_content, collection_name=MONGO_COLLECTION_EMAIL_CONTENT, id_field_name="msg_id")

    def find_all_build_data(self):
        return super().find_all(collection_name=MONGO_COLLECTION_JENKINS_REPORTS)

    def find_and_validate_all_build_data(self):
        result = []
        docs = self.find_all_build_data()
        for doc in docs:
            dic = self._build_data_schema.load(doc, unknown=EXCLUDE)
            result.append(JobBuildData.deserialize(dic))
        return result


class DBWriterEmailContentProcessor(EmailContentProcessor):
    def __init__(self, db: UTResultAggregatorDatabase):
        self._db = db

    def process(self, email_meta: EmailMetaData, lines: List[str]):
        email_content = self._db.find_and_validate_email_content(email_meta.message_id)
        # TODO yarndevtoolsv2 DB: Save email meta to DB // store dates of emails as well to mongodb: Write start date, end date, missing dates between start and end date
        #  builds_with_dates = self._aggregation_results._failed_builds.get_dates()
        if email_content:
            merged_lines: List[str] = DBWriterEmailContentProcessor._merge_lists(
                email_content.lines, lines, return_result_if_first_modified=True
            )
            if merged_lines:
                email_content.lines = merged_lines
                self._db.save_email_content(email_content)
        else:
            self._db.save_email_content(EmailContent.from_message(email_meta, lines))

    @staticmethod
    def _merge_lists(l1, l2, return_result_if_first_modified=False):
        in_first = set(l1)
        in_second = set(l2)
        in_second_but_not_in_first = in_second - in_first
        result = l1 + list(in_second_but_not_in_first)

        if return_result_if_first_modified:
            if in_second_but_not_in_first:
                return result
            else:
                return None
        return result


class JenkinsJobBuildDataAndEmailContentJoiner:
    from yarndevtools.commands.unittestresultaggregator.common.aggregation import AggregationResults

    def __init__(self, db: UTResultAggregatorDatabase):
        self._db = db
        # key: job name, value: Dict[build number, JobBuildData]
        self.fetcher_data_dict: Dict[str, Dict[str, JobBuildData]] = {}
        # key: job name, value: Dict[build number, EmailContent]
        self.aggregator_data_dict: Dict[str, Dict[str, EmailContent]] = {}
        # key: job name, value: set of dates
        self.dates: Dict[str, Set[datetime.date]] = {}

    def join(self, result: AggregationResults):
        # Data from UT result fetcher: failed jenkins builds
        build_data: List[JobBuildData] = self._db.find_and_validate_all_build_data()

        # Data from UT result aggregator: email contents
        email_contents: List[EmailContent] = self._db.find_and_validate_all_email_content()

        for bd in build_data:
            builds_per_job = self.fetcher_data_dict.setdefault(bd.job_name, {})
            builds_per_job.setdefault(bd.build_number, bd)
            self.dates.setdefault(bd.job_name, set())
            self.dates[bd.job_name].add(bd.build_datetime.date())

        if not email_contents:
            raise ValueError(
                "Loaded email contents from DB is empty! Please fill DB by running in execution mode: EMAIL_ONLY"
            )

        for ec in email_contents:
            builds_per_job = self.aggregator_data_dict.setdefault(ec.job_name, {})
            builds_per_job[ec.build_number] = ec
            self.dates.setdefault(ec.job_name, set())
            self.dates[ec.job_name].add(ec.date.date())

        self._do_aggregate(result)

    def _do_aggregate(self, result: AggregationResults):
        self.fetcher_only: Dict[str, Set[str]] = self._get_only_in_first_dict(
            self.fetcher_data_dict, self.aggregator_data_dict
        )
        self.aggregator_only: Dict[str, Set[str]] = self._get_only_in_first_dict(
            self.aggregator_data_dict, self.fetcher_data_dict
        )
        self.from_both: Dict[str, Set[str]] = self._get_from_both_dicts(
            self.fetcher_data_dict, self.aggregator_data_dict
        )

        processed: Set[Tuple[str, str]] = set()  # tuple: (job name, build number)

        self._print_stats()

        for job_name, inner_dict in self.aggregator_data_dict.items():
            for build_number, job_build_data in inner_dict.items():
                item: EmailContent = self.aggregator_data_dict[job_name][build_number]
                failed_build: FailedBuildAbs = FailedBuildAbs.create_from_email_content(item)
                self._process_failed_build(result, failed_build, item.lines)
                processed.add((job_name, build_number))

        for job_name, inner_dict in self.fetcher_data_dict.items():
            for build_number, job_build_data in inner_dict.items():
                key = (job_name, build_number)
                if key not in processed:
                    item: JobBuildData = self.fetcher_data_dict[job_name][build_number]
                    failed_build: FailedBuildAbs = FailedBuildAbs.create_from_job_build_data(item)
                    self._process_failed_build(result, failed_build, job_build_data.testcases)

        result.finish_processing()

    def _is_aggregator_only(self, job_name, build_number):
        if job_name in self.aggregator_only and build_number in self.aggregator_only[job_name]:
            return True
        return False

    def _is_fetcher_only(self, job_name, build_number):
        if job_name in self.fetcher_only and build_number in self.fetcher_only[job_name]:
            return True
        return False

    @staticmethod
    # TODO Make this more abstract and move to pythoncommons?
    def _get_only_in_first_dict(dic_a, dic_b):
        res: Dict[str, Set[str]] = {}
        for job_name, inner_dict in dic_a.items():
            build_numbers_a = inner_dict.keys()
            if job_name not in dic_b:
                res[job_name] = set(build_numbers_a)
            else:
                build_numbers_b = dic_b[job_name].keys()
                for build_number in build_numbers_a:
                    if build_number not in build_numbers_b:
                        res.setdefault(job_name, set()).add(build_number)
        return res

    @staticmethod
    # TODO Make this more abstract and move to pythoncommons?
    def _get_from_both_dicts(dic_a, dic_b):
        res: Dict[str, Set[str]] = {}
        for job_name, inner_dict in dic_a.items():
            build_numbers_a = inner_dict.keys()
            if job_name in dic_b:
                res[job_name] = set()

            build_numbers_b = dic_b[job_name].keys()
            for build_number in build_numbers_a:
                if build_number in build_numbers_b:
                    res.setdefault(job_name, set()).add(build_number)
        return res

    def _print_stats(self):
        self._print_date_stats()
        aggregator_only_formatted = pformat(self.aggregator_only)
        fetcher_only_formatted = pformat(self.fetcher_only)
        from_both_formatted = pformat(self.from_both)
        LOG.debug(
            "Printing found testcase failure statistics: \n"
            "Aggregator only: %s\n"
            "Fetcher only: %s\n"
            "From both: %s",
            aggregator_only_formatted,
            fetcher_only_formatted,
            from_both_formatted,
        )

    def _print_date_stats(self):
        LOG.debug("Printing date statistics. ")
        for job_name, dates in self.dates.items():
            sorted_dates = sorted(dates)
            first_date = sorted_dates[0]
            last_date = sorted_dates[-1]
            all_no_of_days = (last_date - first_date).days
            missing_dates = DateUtils.get_missing_dates(sorted_dates)
            LOG.debug(
                "Job: %s, All days: %s, First date: %s, Last date: %s, Sorted dates: %s, Missing dates: %s",
                job_name,
                all_no_of_days,
                first_date,
                last_date,
                pformat(sorted_dates),
                pformat(missing_dates),
            )

    @staticmethod
    def _process_failed_build(result: AggregationResults, failed_build: FailedBuildAbs, testcases: List[str]):
        LOG.debug("Processing failed build: %s", failed_build.origin())
        testcases = list(map(lambda line: line.strip(), testcases))
        result.start_new_context()
        result.match_testcases(testcases, failed_build.job_name())
        result.finish_context(failed_build)
