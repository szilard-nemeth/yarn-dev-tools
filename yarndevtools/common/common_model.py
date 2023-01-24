from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Set, Any

from marshmallow import Schema, fields, post_load
from pythoncommons.date_utils import DateUtils
from pythoncommons.string_utils import auto_str


@dataclass
class JenkinsTestcaseFilter:
    project_name: str
    filter_expr: str

    @property
    def as_filter_spec(self):
        return f"{self.project_name}:{self.filter_expr}"


@auto_str
class FailedJenkinsBuild:
    def __init__(self, full_url_of_job: str, timestamp: int, job_name):
        split = full_url_of_job.strip("/").rsplit("/")
        self.server_name = self._parse_server_name(full_url_of_job, split)

        self.url = full_url_of_job
        self.urls = JenkinsJobInstanceUrls(full_url_of_job)
        self.build_number = int(split[-1])
        self.timestamp = timestamp
        self.job_name: str = job_name

    @staticmethod
    def _parse_server_name(url, split):
        for s in split:
            if not s.startswith("http") and s:
                return s
        raise ValueError("Failed to parse server name from URL: {}".format(url))

    @property
    def datetime(self):
        return DateUtils.create_datetime_from_timestamp(self.timestamp)


class JobBuildDataStatus(Enum):
    # Invalid statuses
    EMPTY = "Report does not contain testcase data"
    NO_JSON_DATA_FOUND = "No JSON data found for build report"
    CANNOT_FETCH = "Cannot fetch build report"
    ALL_GREEN = "Build report contains tests but all are green"
    # Valid statuses
    HAVE_FAILED_TESTCASES = "Valid build report. Contains some failed tests"


@dataclass
class FilteredResult:
    filter: JenkinsTestcaseFilter
    testcases: List[str]

    def __str__(self):
        tcs = "\n".join(self.testcases)
        s = f"Project: {self.filter.project_name}\n"
        s += f"Filter expression: {self.filter.filter_expr}\n"
        s += f"Number of failed testcases: {len(self.testcases)}\n"
        s += f"Failed testcases (fully qualified name):\n{tcs}"
        return s


class AggregatorEntity(ABC):
    @property
    @abstractmethod
    def job_name(self) -> str:
        pass

    @property
    @abstractmethod
    def build_number(self) -> str:
        pass


class DBSerializable(ABC):
    @abstractmethod
    def serialize(self):
        pass


class JobBuildData(DBSerializable, AggregatorEntity):
    def __init__(self, failed_build: FailedJenkinsBuild, counters, failed_testcases, status: JobBuildDataStatus):
        self._failed_build: FailedJenkinsBuild = failed_build
        self.counters = counters
        self.failed_testcases: List[str] = failed_testcases
        self.filtered_testcases: List[FilteredResult] = []
        self.filtered_testcases_by_expr: Dict[str, List[str]] = {}
        self.no_of_failed_filtered_tc = None
        self.unmatched_testcases: Set[str] = set()
        self.status: JobBuildDataStatus = status
        self._schema = JobBuildDataSchema()

    def serialize(self):
        return self._schema.dump(self)

    def has_failed_testcases(self):
        return len(self.failed_testcases) > 0

    def filter_testcases(self, tc_filters: List[JenkinsTestcaseFilter]):
        matched_testcases = set()
        for tcf in tc_filters:
            filter_expr = tcf.filter_expr
            matched_for_filter = list(filter(lambda tc: filter_expr in tc, self.failed_testcases))
            self.filtered_testcases.append(FilteredResult(tcf, matched_for_filter))

            if filter_expr not in self.filtered_testcases_by_expr:
                self.filtered_testcases_by_expr[filter_expr] = []

            self.filtered_testcases_by_expr[filter_expr].extend(matched_for_filter)
            matched_testcases.update(matched_for_filter)
        self.no_of_failed_filtered_tc = sum([len(fr.testcases) for fr in self.filtered_testcases])
        self.unmatched_testcases = set(self.failed_testcases).difference(matched_testcases)

    @property
    def failed_count(self):
        if not self.counters:
            return -1
        return self.counters.failed

    @property
    def passed_count(self):
        if not self.counters:
            return -1
        return self.counters.passed

    @property
    def skipped_count(self):
        if not self.counters:
            return -1
        return self.counters.skipped

    @property
    def build_number(self):
        return self._failed_build.build_number

    @property
    def build_url(self):
        return self._failed_build.url

    @property
    def build_timestamp(self):
        return self._failed_build.timestamp

    @property
    def build_datetime(self):
        return self._failed_build.datetime

    @property
    def job_name(self):
        return self._failed_build.job_name

    @property
    def is_valid(self):
        return self.status == JobBuildDataStatus.HAVE_FAILED_TESTCASES

    @property
    def tc_filters(self):
        return [res.filter for res in self.filtered_testcases]

    def get_job_name(self) -> str:
        return self.job_name

    def get_build_number(self) -> str:
        return self.build_number

    def __str__(self):
        if self.is_valid:
            return self._str_normal_report()
        else:
            return self._str_invalid_report()

    def _str_invalid_report(self):
        return (
            f"Build number: {self.build_number}\n"
            f"Build URL: {self.build_url}\n"
            f"Invalid report! Details: {self.status.value}\n"
        )

    def _str_normal_report(self):
        filtered_testcases: str = ""
        if self.tc_filters:
            for idx, ftcs in enumerate(self.filtered_testcases):
                filtered_testcases += f"\nFILTER #{idx + 1}\n{str(ftcs)}\n"
        if filtered_testcases:
            filtered_testcases = f"\n{filtered_testcases}\n"

        all_failed_testcases = "\n".join(self.failed_testcases)
        unmatched_testcases = "\n".join(self.unmatched_testcases)
        return (
            f"Counters:\n"
            f"{self.counters}, "
            f"Build number: {self.build_number}\n"
            f"Build URL: {self.build_url}\n"
            f"Matched testcases: {self.no_of_failed_filtered_tc}\n"
            f"Unmatched testcases: {len(self.unmatched_testcases)}\n"
            f"{filtered_testcases}\n"
            f"Unmatched testcases:\n{unmatched_testcases}\n"
            f"ALL Failed testcases:\n{all_failed_testcases}"
        )


class JobBuildDataSchema(Schema):
    build_number = fields.Int(required=True)
    build_url = fields.Str(required=True)
    build_timestamp = fields.Int(required=True)
    status = fields.Enum(JobBuildDataStatus, required=True)
    failed_testcases = fields.List(fields.Str)
    failed_count = fields.Int(required=True)
    passed_count = fields.Int(required=True)
    skipped_count = fields.Int(required=True)
    job_name = fields.Str(required=True)

    @post_load
    def make_job_build_data(self, data, **kwargs):
        return self.deserialize(data)

    @staticmethod
    def deserialize(dic: Dict[Any, Any]):
        # TODO Is there a best practice over this manual field mapping?
        special_vars = [
            "failed_count",
            "passed_count",
            "skipped_count",
            "build_number",
            "build_url",
            "job_name",
            "build_timestamp",
        ]
        normal_keys = set(dic.keys()).difference(special_vars)
        normal_keys_dict = {k: dic[k] for k in normal_keys}
        normal_keys_dict["counters"] = JobBuildDataCounters(
            dic["failed_count"], dic["passed_count"], dic["skipped_count"]
        )

        normal_keys_dict["failed_build"] = FailedJenkinsBuild(
            full_url_of_job=dic["build_url"],
            timestamp=dic["build_timestamp"],
            job_name=dic["job_name"],
        )
        build_data = JobBuildData(**normal_keys_dict)

        # process special vars
        # TODO yarndevtoolsv2 DB: How to reconstruct filtered testcases? Is this required?

        return build_data


class JenkinsJobInstanceUrls:
    # Example URL: http://build.infra.cloudera.com/job/Mawo-UT-hadoop-CDPD-7.x/191/
    def __init__(self, full_url):
        self.full_url = full_url
        self.job_console_output_url = self._append_to_url(full_url, "Console")
        self.test_report_url = self._append_to_url(full_url, "testReport")
        self.test_report_api_json_url = self.test_report_url + "/api/json?pretty=true"

    @staticmethod
    def _append_to_url(full_url, to_append):
        if not full_url[-1] == "/":
            full_url += "/"
        return full_url + to_append


@dataclass
class JobBuildDataCounters:
    failed: int
    passed: int
    skipped: int

    def __str__(self):
        return f"Failed: {self.failed}, Passed: {self.passed}, Skipped: {self.skipped}"


class JenkinsJobUrl:
    # TODO Check other Jenkins* classes and see if something could be extracted
    def __init__(self, raw_url):
        self._raw_url = raw_url

        segments = self._raw_url.split("/job/")
        if len(segments) != 2:
            raise ValueError("Cannot parse job name from Jenkins build URL: {}".format(self._raw_url))

        job_name_and_number = list(filter(lambda i: i, segments[1].split("/")))
        if len(job_name_and_number) != 2:
            raise ValueError(
                "Unexpected Jenkins build URL: {}. Job name and number should be 2-sized list: {}".format(
                    self._raw_url, job_name_and_number
                )
            )

        self.job_name = job_name_and_number[0]
        self.build_number = job_name_and_number[1]
