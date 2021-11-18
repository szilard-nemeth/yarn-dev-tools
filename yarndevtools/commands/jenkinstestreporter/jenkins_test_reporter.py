#!/usr/local/bin/python3
import os
import sys
import traceback
import logging
import time
from dataclasses import dataclass
from typing import List, Dict, Set, Tuple

from pythoncommons.constants import ExecutionMode
from pythoncommons.date_utils import DateUtils
from pythoncommons.email import EmailService, EmailMimeType
from pythoncommons.file_utils import FileUtils, JsonFileUtils
from pythoncommons.logging_setup import SimpleLoggingSetup
from pythoncommons.network_utils import NetworkUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.pickle_utils import PickleUtils
from pythoncommons.project_utils import ProjectUtils
from pythoncommons.string_utils import auto_str

from yarndevtools.argparser import CommandType, JenkinsTestReporterMode, JENKINS_BUILDS_EXAMINE_UNLIMITIED_VAL
from yarndevtools.common.shared_command_utils import FullEmailConfig

from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME

LOG = logging.getLogger(__name__)
EMAIL_SUBJECT_PREFIX = "YARN Daily unit test report:"
CACHED_DATA_FILENAME = "pickled_unit_test_reporter_data.obj"
SECONDS_PER_DAY = 86400
DEFAULT_REQUEST_LIMIT = 999


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
        self.job_console_output_url = full_url + "Console"
        self.test_report_url = full_url + "testReport"
        self.test_report_api_json_url = self.test_report_url + "/api/json?pretty=true"


class FailedJenkinsBuild:
    def __init__(self, full_url_of_job: str, timestamp: int, job_name):
        self.url = full_url_of_job
        self.urls = JenkinsJobInstanceUrls(full_url_of_job)
        self.build_number = full_url_of_job.rsplit("/")[-2]
        self.timestamp = timestamp
        self.job_name = job_name


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


@dataclass
class TestcaseFilter:
    project_name: str
    filter_expr: str

    @property
    def as_filter_spec(self):
        return f"{self.project_name}:{self.filter_expr}"


@auto_str
class DownloadProgress:
    # TODO Store awaiting download / awaiting cache load separately
    def __init__(self, number_of_failed_builds):
        self.all_builds: int = number_of_failed_builds
        self.current_build_idx = 0

    def process_next_build(self):
        self.current_build_idx += 1

    def short_str(self):
        return f"{self.current_build_idx + 1}/{self.all_builds}"


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

    def print_report(self):
        LOG.info(f"\nAmong {self.total_no_of_builds} runs examined, all failed tests <#failedRuns: testName>:")
        # Print summary section: all failed tests sorted by how many times they failed
        LOG.info("TESTCASE SUMMARY:")
        for tn in sorted(self.all_failing_tests, key=self.all_failing_tests.get, reverse=True):
            LOG.info(f"{self.all_failing_tests[tn]}: {tn}")


class JobBuildData:
    def __init__(self, failed_build: FailedJenkinsBuild, counters, testcases, empty_or_not_found=False):
        self._failed_build: FailedJenkinsBuild = failed_build
        self.counters = counters
        self.testcases: List[str] = testcases
        self.filtered_testcases: List[FilteredResult] = []
        self.filtered_testcases_by_expr: Dict[str, List[str]] = {}
        self.no_of_failed_filtered_tc = None
        self.unmatched_testcases: Set[str] = set()
        self.empty_or_not_found = empty_or_not_found
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
        return not self.empty_or_not_found

    @property
    def is_mail_sent(self):
        return self.mail_sent

    @property
    def tc_filters(self):
        return [res.filter for res in self.filtered_testcases]

    def __str__(self):
        if self.empty_or_not_found:
            return self._str_empty_report()
        else:
            return self._str_normal_report()

    def _str_empty_report(self):
        return (
            f"Build number: {self.build_number}\n"
            f"Build URL: {self.build_url}\n"
            f"!!REPORT WAS NOT FOUND OR IT IS EMPTY!!\n"
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


class JobBuildDataCounters:
    def __init__(self, failed, passed, skipped):
        self.failed = failed
        self.passed = passed
        self.skipped = skipped

    def __str__(self):
        return f"Failed: {self.failed}, Passed: {self.passed}, Skipped: {self.skipped}"


class JenkinsTestReporterCache:
    def __init__(self, config):
        self.config: JenkinsTestReporterCacheConfig = config

    def generate_file_name_for_report(self, failed_build: FailedJenkinsBuild):
        job_name = failed_build.job_name.replace(".", "_")
        job_dir_path = FileUtils.ensure_dir_created(FileUtils.join_path(self.config.reports_dir, job_name))
        return FileUtils.join_path(job_dir_path, f"{failed_build.build_number}-testreport.json")

    def is_build_data_downloaded(self, failed_build: FailedJenkinsBuild):
        target_file_path = self.generate_file_name_for_report(failed_build)
        if os.path.exists(target_file_path):
            LOG.debug(
                "Build found in cache. Job name: %s, Build number: %s", failed_build.job_name, failed_build.build_number
            )
            return True, target_file_path
        return False, target_file_path

    def load_cached_data(self) -> Dict[str, JenkinsJobReport]:
        LOG.info("Trying to load cached data from file: %s", self.config.data_file_path)
        if FileUtils.does_file_exist(self.config.data_file_path):
            reports: Dict[str, JenkinsJobReport] = PickleUtils.load(self.config.data_file_path)
            LOG.info("Printing email send status for jobs and builds...")
            for job_name, jenkins_job_report in reports.items():
                for job_url, job_build_data in jenkins_job_report.jobs_by_url.items():
                    LOG.info("Job URL: %s, email sent: %s", job_url, job_build_data.mail_sent)
            LOG.info("Loaded cached data from: %s", self.config.data_file_path)
            return reports
        else:
            LOG.info("Cached data file not found in: %s", self.config.data_file_path)
            return {}

    def dump_data_to_cache(self, reports: Dict[str, JenkinsJobReport], log: bool = False):
        if log:
            LOG.debug("Final cached data object: %s", reports)
        LOG.info("Dumping %s object to file %s", JenkinsJobReport.__name__, self.config.data_file_path)
        PickleUtils.dump(reports, self.config.data_file_path)


class JenkinsTestReporterCacheConfig:
    def __init__(self, args, output_dir):
        self.enabled: bool = not args.disable_file_cache
        self.reports_dir = FileUtils.ensure_dir_created(FileUtils.join_path(output_dir, "reports"))
        self.cached_data_dir = FileUtils.ensure_dir_created(FileUtils.join_path(output_dir, "cached_data"))
        self.download_uncached_job_data: bool = (
            args.download_uncached_job_data if hasattr(args, "download_uncached_job_data") else False
        )

    @property
    def data_file_path(self):
        return FileUtils.join_path(self.cached_data_dir, CACHED_DATA_FILENAME)


class JenkinsTestReporterConfig:
    def __init__(self, output_dir: str, args):
        self.args = args
        self.cache: JenkinsTestReporterCacheConfig = JenkinsTestReporterCacheConfig(args, output_dir)
        self.request_limit = args.req_limit if hasattr(args, "req_limit") and args.req_limit else 1
        self.full_email_conf: FullEmailConfig = FullEmailConfig(args)
        self.jenkins_mode: JenkinsTestReporterMode = (
            JenkinsTestReporterMode[args.jenkins_mode.upper()]
            if hasattr(args, "jenkins_mode") and args.jenkins_mode
            else None
        )
        self.jenkins_base_url = args.jenkins_url
        self.job_names: List[str] = args.job_names.split(",")
        self.num_builds: int = self._determine_number_of_builds_to_examine(args.num_builds, self.request_limit)
        tc_filters_raw = args.tc_filters if hasattr(args, "tc_filters") and args.tc_filters else []
        self.tc_filters: List[TestcaseFilter] = [TestcaseFilter(*tcf.split(":")) for tcf in tc_filters_raw]
        self.session_dir = ProjectUtils.get_session_dir_under_child_dir(FileUtils.basename(output_dir))
        self.full_cmd: str = OsUtils.determine_full_command_filtered(filter_password=True)
        self.force_download_mode = args.force_download_mode if hasattr(args, "force_download_mode") else False
        skip_email = args.skip_email if hasattr(args, "skip_email") else False
        self.force_send_email = args.force_send_email if hasattr(args, "force_send_email") else False
        self.send_mail: bool = not skip_email or not self.force_send_email
        self.omit_job_summary: bool = args.omit_job_summary if hasattr(args, "omit_job_summary") else False
        self.reset_email_sent_state: List[str] = (
            args.reset_sent_state_for_jobs if hasattr(args, "reset_sent_state_for_jobs") else []
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

        if not all([reset in self.job_names for reset in self.reset_email_sent_state]):
            raise ValueError(
                "Not all jobs are recognized while trying to reset email sent state for jobs! "
                "Valid job names: {}, Current job names: {}".format(self.job_names, self.reset_email_sent_state)
            )

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
            f"Force email send mode: {self.force_send_email}\n"
        )


# TODO Move all cache handling related stuff to new class
# TODO Separate all email functionality: Config, email send, etc?
# TODO Separate all download functionality: Progress of downloads, code that fetches API, etc.
class JenkinsTestReporter:
    def __init__(self, args, output_dir):
        self.config = JenkinsTestReporterConfig(output_dir, args)
        self.email_service = EmailService(self.config.full_email_conf.email_conf)
        self.reports: Dict[str, JenkinsJobReport] = {}  # key is the Jenkins job name
        self.cache: JenkinsTestReporterCache = JenkinsTestReporterCache(self.config.cache)

    def run(self):
        LOG.info("Starting Jenkins test reporter. Details: %s", str(self.config))
        SimpleLoggingSetup.init_logger(
            project_name=CommandType.JENKINS_TEST_REPORTER.value,
            logger_name_prefix=YARNDEVTOOLS_MODULE_NAME,
            execution_mode=ExecutionMode.PRODUCTION,
            console_debug=self.config.args.debug,
            postfix=self.config.args.command,
            repos=None,
            verbose_git_log=self.config.args.verbose,
        )
        if self.config.force_download_mode:
            LOG.info("FORCE DOWNLOAD MODE is on")
        else:
            self.reports = self.cache.load_cached_data()

        # Try to reset email sent state of asked jobs
        if self.config.reset_email_sent_state:
            LOG.info("Resetting email sent state to False on these jobs: %s", self.config.reset_email_sent_state)
            for job_name in self.config.reset_email_sent_state:
                self.reports[job_name].reset_mail_sent_state()

        for job_name in self.config.job_names:
            report: JenkinsJobReport = self._create_jenkins_report(job_name)
            self.reports[job_name] = report
            self._process_jenkins_report(report, fail_on_empty_report=False)
        self.cache.dump_data_to_cache(self.reports)

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

    def _process_jenkins_report(self, report, fail_on_empty_report: bool = True):
        report.start_processing()
        if not self.config.send_mail:
            LOG.info("Skip sending email, as per configuration.")

        for i, build_data in enumerate(report):
            LOG.info(f"Processing report of build: {build_data.build_url}")
            if fail_on_empty_report and len(report.all_failing_tests) == 0 and build_data.is_valid:
                LOG.info(
                    f"Report with URL {build_data.build_url} is valid but does not contain any failed tests. "
                    f"Won't process further, exiting..."
                )
                # TODO We don't want to exit here in case of multile reports!
                raise SystemExit(0)

            # At this point it's certain that we have some failed tests or the build itself is invalid
            LOG.info(f"Report of build {build_data.build_url} is not valid or contains failed tests!")
            if not self.config.omit_job_summary and build_data.is_valid:
                report.print_report()
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
            log_report = i == len(report) - 1
            self.cache.dump_data_to_cache(self.reports, log=log_report)

    def download_test_report(self, failed_build: FailedJenkinsBuild, target_file_path):
        url = failed_build.urls.test_report_api_json_url
        LOG.info(f"Loading test report from URL: {url}. " f"Download progress: {self.download_progress.short_str()}")
        data = NetworkUtils.fetch_json(
            url,
            do_not_raise_http_statuses={404},
            http_callbacks={404: lambda: LOG.error(f"Test report cannot be found for build URL (HTTP 404): {url}")},
        )
        if target_file_path:
            LOG.info(f"Saving test report response JSON to cache: {target_file_path}")
            JsonFileUtils.write_data_to_file_as_json(target_file_path, data)
        return data

    def find_failing_tests(self, failed_build: FailedJenkinsBuild):
        """ Find the names of any tests which failed in the given build output URL. """
        try:
            data, loaded_from_cache = self.gather_report_data_for_build(failed_build)
        except Exception:
            traceback.print_exc()
            LOG.error(
                "Could not open test report, check %s for reason why it was reported failed",
                failed_build.urls.job_console_output_url,
            )
            return JobBuildData(failed_build, None, set()), False
        if not data or len(data) == 0:
            return JobBuildData(failed_build, None, [], empty_or_not_found=True), loaded_from_cache

        return self.parse_job_data(data, failed_build), loaded_from_cache

    def gather_report_data_for_build(self, failed_build: FailedJenkinsBuild):
        if self.config.cache.enabled:
            found_in_cache, target_file_path = self.cache.is_build_data_downloaded(failed_build)
            if found_in_cache:
                LOG.info(f"Loading cached test report from file: {target_file_path}")
                data = JsonFileUtils.load_data_from_json_file(target_file_path)
                return data, True
            else:
                data = self.download_test_report(failed_build, target_file_path)
                return data, False
        else:
            data = self.download_test_report(failed_build, None)
            return data, False

    @staticmethod
    def parse_job_data(data, failed_build: FailedJenkinsBuild) -> JobBuildData:
        failed_testcases = set()
        for suite in data["suites"]:
            for case in suite["cases"]:
                status = case["status"]
                err_details = case["errorDetails"]
                if status == "REGRESSION" or status == "FAILED" or (err_details is not None):
                    failed_testcases.add(f"{case['className']}.{case['name']}")
        if len(failed_testcases) == 0:
            LOG.info(
                f"No failed tests in test report, check {failed_build.urls.job_console_output_url} for why it was reported failed."
            )
            return JobBuildData(failed_build, None, failed_testcases)
        else:
            counters = JobBuildDataCounters(data["failCount"], data["passCount"], data["skipCount"])
            return JobBuildData(failed_build, counters, failed_testcases)

    def _create_jenkins_report(self, job_name: str) -> JenkinsJobReport:
        """ Iterate runs of specified job within num_builds and collect results """
        # TODO Discrepancy: request limit vs. days parameter
        jenkins_urls: JenkinsJobUrls = JenkinsJobUrls(self.config.jenkins_base_url, job_name)
        self.failed_builds, self.total_no_of_builds = JenkinsApiConverter.convert(
            job_name, jenkins_urls, days=DEFAULT_REQUEST_LIMIT
        )
        job_datas: List[JobBuildData] = []
        tc_to_fail_count: Dict[str, int] = {}
        sent_requests: int = 0
        # TODO This seems to be wrong, len(failed_builds) is not the same number of builds that should be downloaded
        #  as some of the builds can be cached. TODO: Take the cache into account
        self.download_progress = DownloadProgress(len(self.failed_builds))
        for failed_build in self.failed_builds:
            if sent_requests >= self.config.request_limit:
                LOG.error(f"Reached request limit: {sent_requests}")
                break

            download_build = False
            job_added_from_cache = False
            if (
                self.config.cache.download_uncached_job_data
                and not self.cache.is_build_data_downloaded(failed_build)[0]
            ):
                download_build = True

            # Try to get build data from cache, if found, jump to next build URL
            if self._should_load_build_data_from_cache(failed_build):
                LOG.info("Found build in cache, skipping: %s", failed_build.url)
                job_data = self.reports[job_name].jobs_by_url[failed_build.url]
                job_datas.append(job_data)
                self._create_testcase_to_fail_count_dict(job_data, tc_to_fail_count)
                job_added_from_cache = True

            # We would like to download job data if:
            # 1. job is not found in cache and config.download_uncached_job_data is True OR
            # 2. when job data is not found in file cache.
            if download_build or not job_added_from_cache:
                fmt_timestamp: str = DateUtils.format_unix_timestamp(failed_build.timestamp)
                LOG.info(f"===>{failed_build.urls.test_report_url} ({fmt_timestamp})")

                job_data, loaded_from_cache = self.find_failing_tests(failed_build)

                if not job_added_from_cache:
                    job_data.filter_testcases(self.config.tc_filters)
                    job_datas.append(job_data)
                    self._create_testcase_to_fail_count_dict(job_data, tc_to_fail_count)
                self.download_progress.process_next_build()
                if not loaded_from_cache:
                    sent_requests += 1

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

    def send_mail(self, build_data: JobBuildData):
        if not self.config.omit_job_summary:
            LOG.info(f"\nPRINTING REPORT: \n\n{build_data}")
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
            email_subject = (
                f"{EMAIL_SUBJECT_PREFIX} Failed to fetch test report, " f"build is invalid: {build_data.build_url}"
            )
        return email_subject
