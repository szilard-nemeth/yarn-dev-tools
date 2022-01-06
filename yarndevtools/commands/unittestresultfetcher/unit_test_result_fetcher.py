#!/usr/local/bin/python3
import os
import sys
import traceback
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Set, Tuple, Any

from googleapiwrapper.common import ServiceType
from googleapiwrapper.google_auth import GoogleApiAuthorizer
from googleapiwrapper.google_drive import (
    DriveApiWrapper,
    DriveApiScope,
    DuplicateFileWriteResolutionMode,
    DriveApiWrapperSessionSettings,
    FileFindMode,
    DriveApiWrapperSingleOperationSettings,
    SearchResultHandlingMode,
    DriveApiFile,
)
from pythoncommons.constants import ExecutionMode
from pythoncommons.date_utils import DateUtils
from pythoncommons.email import EmailService, EmailMimeType
from pythoncommons.file_utils import FileUtils, JsonFileUtils, FindResultType
from pythoncommons.logging_setup import SimpleLoggingSetup
from pythoncommons.network_utils import NetworkUtils
from pythoncommons.object_utils import PickleUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import PROJECTS_BASEDIR_NAME
from pythoncommons.string_utils import auto_str

from tests.cdsw.common.testutils.cdsw_testing_common import SECRET_PROJECTS_DIR
from yarndevtools.common.shared_command_utils import FullEmailConfig, CommandType

from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME

CACHED_DATA_DIRNAME = "cached_data"

LOG = logging.getLogger(__name__)
EMAIL_SUBJECT_PREFIX = "YARN Daily unit test report:"
CACHED_DATA_FILENAME = "pickled_unit_test_reporter_data.obj"
SECONDS_PER_DAY = 86400
DEFAULT_REQUEST_LIMIT = 999
JENKINS_BUILDS_EXAMINE_UNLIMITIED_VAL = "jenkins_examine_unlimited_builds"


class UnitTestResultFetcherMode(Enum):
    JENKINS_MASTER = (
        "jenkins_master",
        "https://master-02.jenkins.cloudera.com/",
        [
            "cdpd-master-Hadoop-Common-Unit",
            "cdpd-master-Hadoop-HDFS-Unit",
            "cdpd-master-Hadoop-MR-Unit",
            "cdpd-master-Hadoop-YARN-Unit",
            "CDH-7.1-maint-Hadoop-Common-Unit",
            "CDH-7.1-maint-Hadoop-HDFS-Unit",
            "CDH-7.1-maint-Hadoop-MR-Unit",
            "CDH-7.1-maint-Hadoop-YARN-Unit",
            "CDH-7.1.7.1000-Hadoop-Common-Unit",
            "CDH-7.1.7.1000-Hadoop-HDFS-Unit",
            "CDH-7.1.7.1000-Hadoop-MR-Unit",
            "CDH-7.1.7.1000-Hadoop-YARN-Unit",
        ],
    )
    MAWO = ("MAWO", "http://build.infra.cloudera.com/", ["Mawo-UT-hadoop-CDPD-7.x", "Mawo-UT-hadoop-CDPD-7.1.x"])

    def __init__(self, mode_name: str, jenkins_base_url: str, job_names: List[str]):
        self.mode_name = mode_name
        self.jenkins_base_url = jenkins_base_url
        self.job_names = job_names


class UnitTestResultFetcherCacheType(Enum):
    FILE = "FILE"
    GOOGLE_DRIVE = "GOOGLE_DRIVE"


@auto_str
class DownloadProgress:
    # TODO Store awaiting download / awaiting cache load separately
    # TODO Decide on startup: What build need to be downloaded, what is in the cache, etc.
    def __init__(self, number_of_failed_builds):
        self.all_builds: int = number_of_failed_builds
        self.current_build_idx = 0

    def process_next_build(self):
        self.current_build_idx += 1

    def short_str(self):
        return f"{self.current_build_idx + 1}/{self.all_builds}"


@dataclass
class TestcaseFilter:
    project_name: str
    filter_expr: str

    @property
    def as_filter_spec(self):
        return f"{self.project_name}:{self.filter_expr}"


@auto_str
class FailedJenkinsBuild:
    def __init__(self, full_url_of_job: str, timestamp: int, job_name):
        self.url = full_url_of_job
        self.urls = JenkinsJobInstanceUrls(full_url_of_job)
        self.build_number = full_url_of_job.rsplit("/")[-2]
        self.timestamp = timestamp
        self.job_name: str = job_name


class JobBuildDataStatus(Enum):
    # Invalid statuses
    EMPTY = "Report does not contain testcase data"
    NO_JSON_DATA_FOUND = "No JSON data found for build report"
    CANNOT_FETCH = "Cannot fetch build report"
    ALL_GREEN = "Build report contains tests but all are green"
    # Valid statuses
    HAVE_FAILED_TESTCASES = "Valid build report. Contains some failed tests"


class JobBuildData:
    def __init__(self, failed_build: FailedJenkinsBuild, counters, testcases, status: JobBuildDataStatus):
        self._failed_build: FailedJenkinsBuild = failed_build
        self.counters = counters
        self.testcases: List[str] = testcases
        self.filtered_testcases: List[FilteredResult] = []
        self.filtered_testcases_by_expr: Dict[str, List[str]] = {}
        self.no_of_failed_filtered_tc = None
        self.unmatched_testcases: Set[str] = set()
        self.status: JobBuildDataStatus = status
        # TODO Save this to separate pickled object, so when JobBuildData's structure changes, we don't lose sent state for all jobs
        self.mail_sent = False
        self.sent_date = None

    def has_failed_testcases(self):
        return len(self.testcases) > 0

    def filter_testcases(self, tc_filters: List[TestcaseFilter]):
        matched_testcases = set()
        for tcf in tc_filters:
            matched_for_filter = list(filter(lambda tc: tcf.filter_expr in tc, self.testcases))
            self.filtered_testcases.append(FilteredResult(tcf, matched_for_filter))
            if tcf.filter_expr not in self.filtered_testcases_by_expr:
                self.filtered_testcases_by_expr[tcf.filter_expr] = []
            self.filtered_testcases_by_expr[tcf.filter_expr].extend(matched_for_filter)
            matched_testcases.update(matched_for_filter)
        self.no_of_failed_filtered_tc = sum([len(fr.testcases) for fr in self.filtered_testcases])
        self.unmatched_testcases = set(self.testcases).difference(matched_testcases)

    @property
    def build_number(self):
        return self._failed_build.build_number

    @property
    def build_url(self):
        return self._failed_build.url

    @property
    def is_valid(self):
        return self.status == JobBuildDataStatus.HAVE_FAILED_TESTCASES

    @property
    def is_mail_sent(self):
        return self.mail_sent

    @property
    def tc_filters(self):
        return [res.filter for res in self.filtered_testcases]

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

        all_failed_testcases = "\n".join(self.testcases)
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


class JenkinsJobUrls:
    def __init__(self, jenkins_base_url, job_name):
        self.jenkins_base_url = jenkins_base_url
        self.list_builds = self._get_jenkins_list_builds_url(job_name)

    def _get_jenkins_list_builds_url(self, job_name: str) -> str:
        jenkins_url = self.jenkins_base_url
        if jenkins_url.endswith("/"):
            jenkins_url = jenkins_url[:-1]
        return f"{jenkins_url}/job/{job_name}/api/json?tree=builds[url,result,timestamp]"


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


class JenkinsApiConverter:
    @staticmethod
    def convert(job_name: str, jenkins_urls: JenkinsJobUrls, days: int):
        all_builds: List[Dict[str, str]] = JenkinsApiConverter._list_builds(jenkins_urls)
        last_n_builds: List[Dict[str, str]] = JenkinsApiConverter._filter_builds_last_n_days(all_builds, days=days)
        last_n_failed_build_tuples: List[Tuple[str, int]] = JenkinsApiConverter._get_failed_build_urls_with_timestamps(
            last_n_builds
        )
        failed_build_data: List[Tuple[str, int]] = sorted(
            last_n_failed_build_tuples, key=lambda tup: tup[1], reverse=True
        )
        failed_builds = [
            FailedJenkinsBuild(
                full_url_of_job=tup[0],
                timestamp=JenkinsApiConverter._convert_to_unix_timestamp(tup[1]),
                job_name=job_name,
            )
            for tup in failed_build_data
        ]

        total_no_of_builds = len(all_builds)
        LOG.info(
            f"There are {len(failed_build_data)} builds "
            f"(out of {total_no_of_builds}) that have failed tests "
            f"in the past {days} days. "
            f"Listing builds: {failed_build_data}"
        )
        return failed_builds, total_no_of_builds

    @staticmethod
    def _list_builds(urls: JenkinsJobUrls):
        """ List all builds of the target project. """
        url = urls.list_builds
        try:
            LOG.info("Fetching builds from Jenkins in url: %s", url)
            return NetworkUtils.fetch_json(url)["builds"]
        except Exception:
            LOG.error(f"Could not fetch: {url}")
            raise

    @staticmethod
    def _filter_builds_last_n_days(builds, days):
        # Select only those in the last N days
        min_time = int(time.time()) - SECONDS_PER_DAY * days
        return [b for b in builds if (JenkinsApiConverter._convert_to_unix_timestamp_from_json(b)) > min_time]

    @staticmethod
    def _get_failed_build_urls_with_timestamps(builds):
        return [(b["url"], b["timestamp"]) for b in builds if (b["result"] in ("UNSTABLE", "FAILURE"))]

    @staticmethod
    def _convert_to_unix_timestamp_from_json(build_json):
        timestamp_str = build_json["timestamp"]
        return JenkinsApiConverter._convert_to_unix_timestamp(int(timestamp_str))

    @staticmethod
    def _convert_to_unix_timestamp(ts: int):
        # Jenkins' uses milliseconds format to store the timestamp, divide it by 1000
        # See: https://stackoverflow.com/a/24308978/1106893
        return int(ts / 1000)

    @staticmethod
    def parse_job_data(data, failed_build: FailedJenkinsBuild) -> JobBuildData:
        failed_testcases = set()
        found_testcases: int = 0
        for suite in data["suites"]:
            for tc in suite["cases"]:
                found_testcases += 1
                status = tc["status"]
                err_details = tc["errorDetails"]
                if status == "REGRESSION" or status == "FAILED" or (err_details is not None):
                    failed_testcases.add(f"{tc['className']}.{tc['name']}")
        if len(failed_testcases) == 0:
            if found_testcases:
                LOG.info(
                    f"No failed tests in test report, check {failed_build.urls.job_console_output_url} for why it was reported failed."
                )
                return JobBuildData(failed_build, None, failed_testcases, status=JobBuildDataStatus.ALL_GREEN)
            else:
                return JobBuildData(failed_build, None, failed_testcases, status=JobBuildDataStatus.EMPTY)
        else:
            counters = JobBuildDataCounters(data["failCount"], data["passCount"], data["skipCount"])
            return JobBuildData(
                failed_build, counters, failed_testcases, status=JobBuildDataStatus.HAVE_FAILED_TESTCASES
            )

    @staticmethod
    def download_test_report(failed_build: FailedJenkinsBuild, download_progress: DownloadProgress):
        url = failed_build.urls.test_report_api_json_url
        LOG.info(f"Loading test report from URL: {url}. Download progress: {download_progress.short_str()}")
        data = NetworkUtils.fetch_json(
            url,
            do_not_raise_http_statuses={404},
            http_callbacks={404: lambda: LOG.error(f"Test report cannot be found for build URL (HTTP 404): {url}")},
        )
        return data


@dataclass
class FilteredResult:
    filter: TestcaseFilter
    testcases: List[str]

    def __str__(self):
        tcs = "\n".join(self.testcases)
        s = f"Project: {self.filter.project_name}\n"
        s += f"Filter expression: {self.filter.filter_expr}\n"
        s += f"Number of failed testcases: {len(self.testcases)}\n"
        s += f"Failed testcases (fully qualified name):\n{tcs}"
        return s


@auto_str
class JenkinsJobReport:
    def __init__(self, job_build_datas, all_failing_tests, total_no_of_builds: int, num_builds_per_config: int):
        self.jobs_by_url: Dict[str, JobBuildData] = {job.build_url: job for job in job_build_datas}
        # Sort by URL, descending
        self._job_urls = list(sorted(self.jobs_by_url.keys(), reverse=True))
        self.all_failing_tests: Dict[str, int] = all_failing_tests
        self.total_no_of_builds: int = total_no_of_builds
        self.actual_num_builds = self._determine_actual_number_of_builds(num_builds_per_config)
        self._index = 0

    def start_processing(self):
        LOG.info(f"Report list contains build results: {self._job_urls}")
        LOG.info(f"Processing {self.actual_num_builds} in Report...")

    def __len__(self):
        return self.actual_num_builds

    def __iter__(self):
        return self

    def __next__(self):
        if self._index == self.actual_num_builds:
            raise StopIteration
        result = self.jobs_by_url[self._job_urls[self._index]]
        self._index += 1
        return result

    def _determine_actual_number_of_builds(self, num_builds_per_config):
        build_data_count = len(self.jobs_by_url)
        total_no_of_builds = self.total_no_of_builds
        if build_data_count < total_no_of_builds:
            LOG.warning(
                "Report contains less builds than total number of builds. " "Report has: %d, Total: %d",
                build_data_count,
                total_no_of_builds,
            )
            actual_num_builds = min(num_builds_per_config, build_data_count)
        else:
            actual_num_builds = min(num_builds_per_config, self.total_no_of_builds)
        return actual_num_builds

    @property
    def known_build_urls(self):
        return self.jobs_by_url.keys()

    def are_all_mail_sent(self):
        return all(job_data.mail_sent for job_data in self.jobs_by_url.values())

    def reset_mail_sent_state(self):
        for job_data in self.jobs_by_url.values():
            job_data.sent_date = None
            job_data.mail_sent = False

    def mark_sent(self, build_url):
        job_data = self.jobs_by_url[build_url]
        job_data.sent_date = DateUtils.get_current_datetime()
        job_data.mail_sent = True

    def get_job_data(self, build_url: str):
        return self.jobs_by_url[build_url]

    def print_report(self, build_data):
        LOG.info(f"\nPRINTING REPORT: \n\n{build_data}")
        LOG.info(f"\nAmong {self.total_no_of_builds} runs examined, all failed tests <#failedRuns: testName>:")
        # Print summary section: all failed tests sorted by how many times they failed
        LOG.info("TESTCASE SUMMARY:")
        for tn in sorted(self.all_failing_tests, key=self.all_failing_tests.get, reverse=True):
            LOG.info(f"{self.all_failing_tests[tn]}: {tn}")


@dataclass
class JobBuildDataCounters:
    failed: int
    passed: int
    skipped: int

    def __str__(self):
        return f"Failed: {self.failed}, Passed: {self.passed}, Skipped: {self.skipped}"


@dataclass(frozen=True)
class CachedBuildKey:
    job_name: str
    build_number: str


@dataclass
class CachedBuild:
    build_key: CachedBuildKey
    full_report_file_path: str


class Cache(ABC):
    @abstractmethod
    def initialize(self) -> Dict[str, JenkinsJobReport]:
        pass

    @abstractmethod
    def is_build_data_in_cache(self, cached_build_key: CachedBuildKey):
        pass

    @abstractmethod
    def load_reports_meta(self) -> Dict[str, JenkinsJobReport]:
        pass

    @abstractmethod
    def save_reports_meta(self, reports: Dict[str, JenkinsJobReport], log: bool = False):
        pass

    @abstractmethod
    def save_report(self, data, cached_build_key: CachedBuildKey):
        pass

    @abstractmethod
    def load_report(self, cached_build_key: CachedBuildKey) -> Dict[Any, Any]:
        pass

    @property
    @abstractmethod
    def meta_file_path(self):
        pass

    @staticmethod
    def generate_job_dirname(cached_build_key: CachedBuildKey):
        return Cache.escape_job_name(cached_build_key.job_name)

    @staticmethod
    def escape_job_name(job_name: str):
        return job_name.replace(".", "_")

    @staticmethod
    def generate_report_filename(cached_build_key: CachedBuildKey):
        return f"{cached_build_key.build_number}-testreport.json"


class FileCache(Cache):
    def __init__(self, config):
        self.config: CacheConfig = config

    def initialize(self) -> Dict[str, JenkinsJobReport]:
        report_files = FileUtils.find_files(
            self.config.reports_dir,
            find_type=FindResultType.FILES,
            single_level=False,
            full_path_result=True,
            extension="json",
        )
        self.cached_builds: Dict[CachedBuildKey, CachedBuild] = self._load_cached_builds_from_fs(report_files)
        LOG.info("Loaded cached builds: %s", self.cached_builds)
        return self.load_reports_meta()

    def _load_cached_builds_from_fs(self, report_files):
        cached_builds: Dict[CachedBuildKey, CachedBuild] = {}
        for report_file in report_files:
            orig_file_path = report_file
            # Example: CDH-7_1-maint-Hadoop-Common-Unit/1-testreport.json
            if report_file.startswith(self.config.reports_dir):
                report_file = report_file[len(self.config.reports_dir) :]
            comps = report_file.split(os.sep)
            comps = [c for c in comps if c]
            job_name = comps[0]
            report_filename = comps[1]
            build_number = report_filename.split("-")[0]
            key = CachedBuildKey(job_name, build_number)
            cached_builds[key] = CachedBuild(key, orig_file_path)
        return cached_builds

    def _generate_file_name_for_report(self, cached_build_key: CachedBuildKey):
        job_dir_path = FileUtils.join_path(self.config.reports_dir, self.generate_job_dirname(cached_build_key))
        job_dir_path = FileUtils.ensure_dir_created(job_dir_path)
        return FileUtils.join_path(job_dir_path, self.generate_report_filename(cached_build_key))

    def is_build_data_in_cache(self, cached_build_key: CachedBuildKey):
        if cached_build_key in self.cached_builds:
            LOG.debug(
                "Build found in cache. Job name: %s, Build number: %s",
                cached_build_key.job_name,
                cached_build_key.build_number,
            )
            return True
        return False

    def load_reports_meta(self) -> Dict[str, JenkinsJobReport]:
        LOG.info("Trying to load cached data from file: %s", self.meta_file_path)
        if FileUtils.does_file_exist(self.meta_file_path):
            reports: Dict[str, JenkinsJobReport] = PickleUtils.load(self.meta_file_path)
            LOG.info("Printing email send status for jobs and builds...")
            for job_name, jenkins_job_report in reports.items():
                for job_url, job_build_data in jenkins_job_report.jobs_by_url.items():
                    LOG.info("Job URL: %s, email sent: %s", job_url, job_build_data.mail_sent)
            LOG.info("Loaded cached data from: %s", self.meta_file_path)
            return reports
        else:
            LOG.info("Cached data file not found in: %s", self.meta_file_path)
            return {}

    def save_reports_meta(self, reports: Dict[str, JenkinsJobReport], log: bool = False):
        if log:
            LOG.debug("Final cached data object: %s", reports)
        LOG.info("Dumping %s object to file %s", JenkinsJobReport.__name__, self.meta_file_path)
        PickleUtils.dump(reports, self.meta_file_path)

    def save_report(self, data, cached_build_key: CachedBuildKey):
        report_file_path = self._generate_file_name_for_report(cached_build_key)
        LOG.info(f"Saving test report response JSON to file cache: {report_file_path}")
        JsonFileUtils.write_data_to_file_as_json(report_file_path, data)
        return report_file_path

    def load_report(self, cached_build_key: CachedBuildKey) -> Dict[Any, Any]:
        report_file_path = self._generate_file_name_for_report(cached_build_key)
        LOG.info(f"Loading cached test report from file: {report_file_path}")
        return JsonFileUtils.load_data_from_json_file(report_file_path)

    @property
    def meta_file_path(self):
        return self.config.data_file_path


class GoogleDriveCache(Cache):
    DRIVE_FINAL_CACHE_DIR = CommandType.UNIT_TEST_RESULT_FETCHER.output_dir_name + "_" + CACHED_DATA_DIRNAME
    # TODO implement throttling: Too many requests to Google Drive?

    def __init__(self, config):
        self.config: CacheConfig = config
        self.file_cache: FileCache = FileCache(config)
        self.authorizer = GoogleApiAuthorizer(
            ServiceType.DRIVE,
            project_name=CommandType.UNIT_TEST_RESULT_FETCHER.output_dir_name,
            secret_basedir=SECRET_PROJECTS_DIR,
            account_email="snemeth@cloudera.com",
            scopes=[DriveApiScope.DRIVE_PER_FILE_ACCESS.value],
        )
        session_settings = DriveApiWrapperSessionSettings(
            FileFindMode.JUST_UNTRASHED, DuplicateFileWriteResolutionMode.ADD_NEW_REVISION, enable_path_cache=True
        )
        self.drive_wrapper = DriveApiWrapper(self.authorizer, session_settings=session_settings)
        self.drive_meta_dir_path = FileUtils.join_path(
            PROJECTS_BASEDIR_NAME, YARNDEVTOOLS_MODULE_NAME, self.DRIVE_FINAL_CACHE_DIR
        )
        self.drive_reports_basedir = FileUtils.join_path(
            PROJECTS_BASEDIR_NAME, YARNDEVTOOLS_MODULE_NAME, self.DRIVE_FINAL_CACHE_DIR, "reports"
        )

    def initialize(self):
        reports = self.file_cache.initialize()
        self._sync_from_file_cache()
        return reports

    def _sync_from_file_cache(self):
        all_report_files: List[DriveApiFile] = self.drive_wrapper.get_files("*-testreport.json")
        found_builds: Set[CachedBuildKey] = set()
        for report_drive_file in all_report_files:
            job_name = report_drive_file._parent.name
            components = report_drive_file.name.split("-")
            if len(components) != 2:
                LOG.error("Found test report with unexpected name: %s", job_name)
                continue
            found_builds.add(CachedBuildKey(job_name, components[0]))
        LOG.debug("Found %d builds from Google Drive: %s", len(found_builds), found_builds)
        builds_to_check_from_drive = {
            key: value for (key, value) in self.file_cache.cached_builds.items() if key not in found_builds
        }
        LOG.debug("Will check these builds in Google Drive: %s", builds_to_check_from_drive)

        # TODO Implement sync from GDrive -> Filesystem (other way around)
        # TODO Create progressTracker object to show current status of Google Drive uploads / queries
        for cached_build_key, cached_build in builds_to_check_from_drive.items():
            drive_report_file_path = self._generate_file_name_for_report(cached_build_key)
            settings: DriveApiWrapperSingleOperationSettings = DriveApiWrapperSingleOperationSettings(
                file_find_mode=None,
                duplicate_file_handling_mode=DuplicateFileWriteResolutionMode.FAIL_FAST,
                search_result_handling_mode=SearchResultHandlingMode.SINGLE_FILE_PER_SEARCH_RESULT,
            )
            exist = self.drive_wrapper.does_file_exist(drive_report_file_path, op_settings=settings)
            if not exist:
                settings: DriveApiWrapperSingleOperationSettings = DriveApiWrapperSingleOperationSettings(
                    file_find_mode=None, duplicate_file_handling_mode=DuplicateFileWriteResolutionMode.FAIL_FAST
                )
                self.drive_wrapper.upload_file(
                    cached_build.full_report_file_path, drive_report_file_path, op_settings=settings
                )

    def _generate_file_name_for_report(self, cached_build_key: CachedBuildKey):
        return FileUtils.join_path(
            self.drive_reports_basedir,
            self.generate_job_dirname(cached_build_key),
            self.generate_report_filename(cached_build_key),
        )

    def is_build_data_in_cache(self, cached_build_key: CachedBuildKey):
        # TODO Check in Drive and if not successful, decide based on local file cache
        return self.file_cache.is_build_data_in_cache(cached_build_key)

    def load_reports_meta(self) -> Dict[str, JenkinsJobReport]:
        # TODO Load from Drive and if not successful, load from local file cache
        return self.file_cache.load_reports_meta()

    def save_reports_meta(self, reports: Dict[str, JenkinsJobReport], log: bool = False):
        # TODO implement throttling: Too many requests to Google Drive
        self.file_cache.save_reports_meta(reports)
        drive_path = FileUtils.join_path(self.drive_meta_dir_path, CACHED_DATA_FILENAME)
        self.drive_wrapper.upload_file(self.meta_file_path, drive_path)

    def save_report(self, data, cached_build_key: CachedBuildKey):
        saved_report_file_path = self.file_cache.save_report(data, cached_build_key)
        drive_path = self._generate_file_name_for_report(cached_build_key)
        self.drive_wrapper.upload_file(saved_report_file_path, drive_path)

    def load_report(self, cached_build_key: CachedBuildKey) -> Dict[Any, Any]:
        cache_hit = self.file_cache.is_build_data_in_cache(cached_build_key)
        if cache_hit:
            return self.file_cache.load_report(cached_build_key)
        else:
            filename = self._generate_file_name_for_report(cached_build_key)
            self.drive_wrapper.get_file(filename)
            # TODO missing return
        # TODO Load from Drive and if not successful, load from local file cache
        # TODO IF report.json is only found in local cache, save it to Drive

    @property
    def meta_file_path(self):
        return self.file_cache.meta_file_path


class CacheConfig:
    def __init__(self, args, output_dir):
        self.enabled: bool = not args.disable_file_cache if hasattr(args, "disable_file_cache") else False
        self.reports_dir = FileUtils.ensure_dir_created(FileUtils.join_path(output_dir, "reports"))
        self.cached_data_dir = FileUtils.ensure_dir_created(FileUtils.join_path(output_dir, CACHED_DATA_DIRNAME))
        self.download_uncached_job_data: bool = (
            args.download_uncached_job_data if hasattr(args, "download_uncached_job_data") else False
        )
        self.cache_type: UnitTestResultFetcherCacheType = (
            UnitTestResultFetcherCacheType(args.cache_type.upper())
            if hasattr(args, "cache_type") and args.cache_type
            else UnitTestResultFetcherCacheType.FILE
        )

    @property
    def data_file_path(self):
        return FileUtils.join_path(self.cached_data_dir, CACHED_DATA_FILENAME)


class EmailConfig:
    def __init__(self, args):
        self.full_email_conf: FullEmailConfig = FullEmailConfig(args, allow_empty_subject=True)
        skip_email = args.skip_email if hasattr(args, "skip_email") else False
        self.force_send_email = args.force_send_email if hasattr(args, "force_send_email") else False
        self.send_mail: bool = not skip_email or self.force_send_email
        self.reset_email_sent_state: List[str] = (
            args.reset_sent_state_for_jobs if hasattr(args, "reset_sent_state_for_jobs") else []
        )
        if not self.send_mail:
            LOG.info("Skip sending emails, as per configuration.")

    def validate(self, job_names: List[str]):
        if not all([reset in job_names for reset in self.reset_email_sent_state]):
            raise ValueError(
                "Not all jobs are recognized while trying to reset email sent state for jobs! "
                "Valid job names: {}, Current job names: {}".format(job_names, self.reset_email_sent_state)
            )


class Email:
    def __init__(self, config):
        self.config: EmailConfig = config
        self.email_service = EmailService(config.full_email_conf.email_conf)

    def initialize(self, reports: Dict[str, JenkinsJobReport]):
        # Try to reset email sent state of asked jobs
        if self.config.reset_email_sent_state:
            LOG.info("Resetting email sent state to False on these jobs: %s", self.config.reset_email_sent_state)
            for job_name in self.config.reset_email_sent_state:
                # Reports can be empty at this point if cache was empty for this job or not found
                if job_name in reports:
                    reports[job_name].reset_mail_sent_state()

    def send_mail(self, build_data: JobBuildData):
        # TODO Add MailSendProgress class to track how many emails were sent
        LOG.info("Sending report in email for job: %s", build_data.build_url)
        self.email_service.send_mail(
            sender=self.config.full_email_conf.sender,
            subject=self._get_email_subject(build_data),
            body=str(build_data),
            recipients=self.config.full_email_conf.recipients,
            body_mimetype=EmailMimeType.PLAIN,
        )
        LOG.info("Finished sending report in email for job: %s", build_data.build_url)

    @staticmethod
    def _get_email_subject(build_data: JobBuildData):
        if build_data.is_valid:
            email_subject = f"{EMAIL_SUBJECT_PREFIX} Failed tests with build: {build_data.build_url}"
        else:
            email_subject = f"{EMAIL_SUBJECT_PREFIX} Error with test report, build is invalid: {build_data.build_url}"
        return email_subject

    def process(self, build_data, report):
        if self.config.send_mail:
            if not build_data.is_mail_sent or self.config.force_send_email:
                self.send_mail(build_data)
                report.mark_sent(build_data.build_url)
            else:
                LOG.info(
                    "Not sending report of job URL %s, as it was already sent before on %s.",
                    build_data.build_url,
                    build_data.sent_date,
                )


class UnitTestResultFetcherConfig:
    def __init__(self, output_dir: str, args):
        self.args = args
        self.cache: CacheConfig = CacheConfig(args, output_dir)
        self.email: EmailConfig = EmailConfig(args)
        self.request_limit = args.req_limit if hasattr(args, "req_limit") and args.req_limit else 1
        self.jenkins_mode: UnitTestResultFetcherMode = (
            UnitTestResultFetcherMode[args.jenkins_mode.upper()]
            if hasattr(args, "jenkins_mode") and args.jenkins_mode
            else None
        )
        self.jenkins_base_url = args.jenkins_url
        self.job_names: List[str] = args.job_names.split(",")
        self.num_builds: int = self._determine_number_of_builds_to_examine(args.num_builds, self.request_limit)
        tc_filters_raw = args.tc_filters if hasattr(args, "tc_filters") and args.tc_filters else []
        self.tc_filters: List[TestcaseFilter] = [TestcaseFilter(*tcf.split(":")) for tcf in tc_filters_raw]
        self.full_cmd: str = OsUtils.determine_full_command_filtered(filter_password=True)
        self.force_download_mode = args.force_download_mode if hasattr(args, "force_download_mode") else False
        self.omit_job_summary: bool = args.omit_job_summary if hasattr(args, "omit_job_summary") else False
        self.fail_on_all_green_report: bool = False  # TODO hardcoded
        self.fail_on_empty_report: bool = False  # TODO hardcoded
        self.fail_reports_with_no_data: bool = False  # TODO hardcoded
        self.reset_job_build_data_for_jobs: List[str] = (
            args.reset_job_build_data_for_jobs if hasattr(args, "reset_job_build_data_for_jobs") else []
        )

        # Validation
        if not self.tc_filters:
            LOG.warning("TESTCASE FILTER IS NOT SET!")
        if self.jenkins_mode and (self.jenkins_base_url or self.job_names):
            LOG.warning(
                "Jenkins mode is set to %s. \n"
                "Specified values for Jenkins URL: %s\n"
                "Specified values for job names: %s\n"
                "Jenkins mode will take precedence!",
                self.jenkins_mode,
                self.jenkins_base_url,
                self.job_names,
            )
            self.jenkins_base_url = self.jenkins_mode.jenkins_base_url
            self.job_names = self.jenkins_mode.job_names

        if not all([reset in self.job_names for reset in self.reset_job_build_data_for_jobs]):
            raise ValueError(
                "Not all jobs are recognized while trying to reset job build data for jobs! "
                "Valid job names: {}, Current job names: {}".format(self.job_names, self.reset_job_build_data_for_jobs)
            )

        self.email.validate(self.job_names)

    @staticmethod
    def _determine_number_of_builds_to_examine(config_value, request_limit) -> int:
        if config_value == JENKINS_BUILDS_EXAMINE_UNLIMITIED_VAL:
            return sys.maxsize

        no_of_builds = int(config_value)
        if request_limit < no_of_builds:
            LOG.warning("Limiting the number of builds to fetch by the request limit: %s", request_limit)
        return min(no_of_builds, request_limit)

    def __str__(self):
        # TODO Add all config properties
        return (
            f"Full command was: {self.full_cmd}\n"
            f"Jenkins URL: {self.jenkins_base_url}\n"
            f"Jenkins job names: {self.job_names}\n"
            f"Number of builds to check: {self.num_builds}\n"
            f"Testcase filters: {self.tc_filters}\n"
            f"Force download mode: {self.force_download_mode}\n"
            f"Force email send mode: {self.email.force_send_email}\n"
        )


# TODO Separate all download functionality: Progress of downloads, code that fetches API, etc.
class UnitTestResultFetcher:
    def __init__(self, args, output_dir):
        self.config = UnitTestResultFetcherConfig(output_dir, args)
        self.reports: Dict[str, JenkinsJobReport] = {}  # key is the Jenkins job name
        self.cache: Cache = self._create_cache(self.config)
        self.email: Email = Email(self.config.email)
        self.sent_requests: int = 0

    @staticmethod
    def _convert_to_cache_build_key(failed_build: FailedJenkinsBuild):
        # Cached build data is stored in dirs with dots replaced by underscores,
        # make CachedBuildKey to follow the dir name pattern, so job_names are always consistent when used in
        # CachedBuildKey.
        job_name = Cache.escape_job_name(failed_build.job_name)
        return CachedBuildKey(job_name, failed_build.build_number)

    @staticmethod
    def _create_cache(config: UnitTestResultFetcherConfig):
        if config.cache.cache_type == UnitTestResultFetcherCacheType.FILE:
            LOG.info("Using file cache.")
            return FileCache(config.cache)
        elif config.cache.cache_type == UnitTestResultFetcherCacheType.GOOGLE_DRIVE:
            LOG.info("Using Google Drive cache.")
            return GoogleDriveCache(config.cache)

    def run(self):
        LOG.info("Starting Jenkins test reporter. Details: %s", str(self.config))
        SimpleLoggingSetup.init_logger(
            project_name=CommandType.UNIT_TEST_RESULT_FETCHER.value,
            logger_name_prefix=YARNDEVTOOLS_MODULE_NAME,
            execution_mode=ExecutionMode.PRODUCTION,
            console_debug=self.config.args.debug,
            postfix=self.config.args.command,
            repos=None,
            verbose_git_log=self.config.args.verbose,
            enable_logging_setup_debug_details=False,
        )
        if self.config.force_download_mode:
            LOG.info("FORCE DOWNLOAD MODE is on")
        elif self.config.cache.enabled:
            self.reports: Dict[str, JenkinsJobReport] = self.cache.initialize()
        for reset_job in self.config.reset_job_build_data_for_jobs:
            LOG.info("Reset job build data for job: %s", reset_job)
            if reset_job in self.reports:
                del self.reports[reset_job]

        self.email.initialize(self.reports)

        self.sent_requests = 0
        for job_name in self.config.job_names:
            report: JenkinsJobReport = self._create_jenkins_report(job_name)
            self.reports[job_name] = report
            self.process_jenkins_report(report)
        self.cache.save_reports_meta(self.reports)

    def _get_report_by_job_name(self, job_name):
        return self.reports[job_name]

    def get_failed_tests(self, job_name) -> List[str]:
        report = self._get_report_by_job_name(job_name)
        if not report:
            raise ValueError("Report is not queried yet or it is None!")
        return list(report.all_failing_tests.keys())

    def get_num_build_data(self, job_name):
        return len(self._get_report_by_job_name(job_name).jobs_by_url)

    @property
    def testcase_filters(self) -> List[str]:
        return [tcf.as_filter_spec for tcf in self.config.tc_filters]

    def get_filtered_testcases_from_build(self, build_url: str, package: str, job_name: str):
        return [
            tc
            for filtered_res in self._get_report_by_job_name(job_name).get_job_data(build_url).filtered_testcases
            for tc in filtered_res.testcases
            if package in tc
        ]

    def process_jenkins_report(self, report: JenkinsJobReport):
        report.start_processing()
        for i, build_data in enumerate(report):
            self._process_build_data_from_report(build_data, report)
            self._print_report(build_data, report)
            self._invoke_report_processors(build_data, report)
            # TODO fix
            # self._save_all_reports_to_cache(i, report)

    def _process_build_data_from_report(self, build_data: JobBuildData, report: JenkinsJobReport):
        LOG.info(f"Processing report of build: {build_data.build_url}")
        should_exit: bool = False
        if build_data.status == JobBuildDataStatus.ALL_GREEN:
            LOG.error(
                "Report with URL %s does exist but does not contain any failed tests as all testcases are green. ",
                build_data.build_url,
            )
            if self.config.fail_on_all_green_report:
                should_exit = True
        elif build_data.status == JobBuildDataStatus.EMPTY:
            LOG.info(
                "Report with URL %s is valid but does not contain any testcase data, A.K.A. empty.",
                build_data.build_url,
            )
            if self.config.fail_on_empty_report:
                should_exit = True
        elif build_data.status in (JobBuildDataStatus.NO_JSON_DATA_FOUND, JobBuildDataStatus.CANNOT_FETCH):
            LOG.info("Report with URL %s but couldn't fetch build or JSON data.", build_data.build_url)
            if self.config.fail_reports_with_no_data:
                should_exit = True

        if should_exit:
            LOG.info("Will not process more reports, exiting...")
            raise SystemExit(0)

    def _print_report(self, build_data: JobBuildData, report: JenkinsJobReport):
        if build_data.is_valid:
            LOG.info("Report of build %s contains failed tests!", build_data.build_url)
        else:
            LOG.info("Report of build %s is not valid! Details: %s", build_data.build_url, build_data.status.value)
        if not self.config.omit_job_summary and build_data.is_valid:
            report.print_report(build_data)

    def _invoke_report_processors(self, build_data: JobBuildData, report: JenkinsJobReport):
        self.email.process(build_data, report)

    def _save_all_reports_to_cache(self, i, report: JenkinsJobReport):
        log_report: bool = i == len(report) - 1
        self.cache.save_reports_meta(self.reports, log=log_report)

    def create_job_build_data(self, failed_build: FailedJenkinsBuild):
        """ Find the names of any tests which failed in the given build output URL. """
        try:
            data = self.gather_raw_data_for_build(failed_build)
        except Exception:
            traceback.print_exc()
            LOG.error(
                "Could not open test report, check %s for reason why it was reported failed",
                failed_build.urls.job_console_output_url,
            )
            return JobBuildData(failed_build, None, set(), status=JobBuildDataStatus.CANNOT_FETCH)
        # TODO If data was loaded from cache and it is still None or len(data) == 0 (e.g. empty or corrupt file)
        #  script will think that report is empty. This case a file download is required.
        if not data or len(data) == 0:
            return JobBuildData(failed_build, None, [], status=JobBuildDataStatus.NO_JSON_DATA_FOUND)

        return JenkinsApiConverter.parse_job_data(data, failed_build)

    def gather_raw_data_for_build(self, failed_build: FailedJenkinsBuild):
        if self.config.cache.enabled:
            cache_build_key = self._convert_to_cache_build_key(failed_build)
            cache_hit = self.cache.is_build_data_in_cache(cache_build_key)
            if cache_hit:
                return self.cache.load_report(cache_build_key)
            else:
                return self._download_build_data(failed_build)
        else:
            return self._download_build_data(failed_build)

    def _download_build_data(self, failed_build):
        fmt_timestamp: str = DateUtils.format_unix_timestamp(failed_build.timestamp)
        LOG.debug(f"Downloading job data from URL: {failed_build.urls.test_report_url}, timestamp: ({fmt_timestamp})")
        data = JenkinsApiConverter.download_test_report(failed_build, self.download_progress)
        self.sent_requests += 1
        if self.config.cache.enabled:
            self.cache.save_report(data, self._convert_to_cache_build_key(failed_build))
        return data

    def _create_jenkins_report(self, job_name: str) -> JenkinsJobReport:
        """ Iterate runs of specified job within num_builds and collect results """
        # TODO Discrepancy: request limit vs. days parameter
        jenkins_urls: JenkinsJobUrls = JenkinsJobUrls(self.config.jenkins_base_url, job_name)
        self.failed_builds, self.total_no_of_builds = JenkinsApiConverter.convert(
            job_name, jenkins_urls, days=DEFAULT_REQUEST_LIMIT
        )
        job_datas: List[JobBuildData] = []
        tc_to_fail_count: Dict[str, int] = {}
        # TODO This seems to be wrong, len(failed_builds) is not the same number of builds that should be downloaded
        #  as some of the builds can be cached. TODO: Take the cache into account
        self.download_progress = DownloadProgress(len(self.failed_builds))
        for failed_build in self.failed_builds:
            if self.sent_requests >= self.config.request_limit:
                LOG.error(f"Reached request limit: {self.sent_requests}")
                break

            download_build = False
            job_added_from_cache = False
            if self.config.cache.download_uncached_job_data and not self.cache.is_build_data_in_cache(
                self._convert_to_cache_build_key(failed_build)
            ):
                download_build = True

            # Try to get build data from cache, if found, jump to next build URL
            if self._should_load_build_data_from_cache(failed_build):
                LOG.info("Found build in cache, skipping: %s", failed_build.url)
                # If job build data was intentionally reset by config option 'reset_job_build_data_for_jobs',
                # build data for job is already removed from the dict 'self.reports'
                if job_name in self.reports:
                    job_data = self.reports[job_name].jobs_by_url[failed_build.url]
                    job_datas.append(job_data)
                    self._create_testcase_to_fail_count_dict(job_data, tc_to_fail_count)
                    job_added_from_cache = True

            # We would like to download job data if:
            # 1. job is not found in cache and config.download_uncached_job_data is True OR
            # 2. when job data is not found in file cache.
            if download_build or not job_added_from_cache:
                job_data = self.create_job_build_data(failed_build)
                if not job_added_from_cache:
                    job_data.filter_testcases(self.config.tc_filters)
                    job_datas.append(job_data)
                    self._create_testcase_to_fail_count_dict(job_data, tc_to_fail_count)
                self.download_progress.process_next_build()

        return JenkinsJobReport(job_datas, tc_to_fail_count, self.total_no_of_builds, self.config.num_builds)

    def _should_load_build_data_from_cache(self, failed_build: FailedJenkinsBuild):
        return (
            not self.config.force_download_mode
            and failed_build.job_name in self.reports
            and failed_build.url in self.reports[failed_build.job_name].known_build_urls
        )

    @staticmethod
    def _create_testcase_to_fail_count_dict(job_data, tc_to_fail_count):
        if job_data.has_failed_testcases():
            for failed_testcase in job_data.testcases:
                LOG.info(f"Failed test: {failed_testcase}")
                tc_to_fail_count[failed_testcase] = tc_to_fail_count.get(failed_testcase, 0) + 1
