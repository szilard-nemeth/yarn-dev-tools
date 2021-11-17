#!/usr/local/bin/python3
import os
import sys
import traceback
import datetime
import json
import logging
import time
from dataclasses import dataclass
from typing import List, Dict, Set, Tuple

from pythoncommons.constants import ExecutionMode
from pythoncommons.date_utils import DateUtils
from pythoncommons.email import EmailService, EmailMimeType
from pythoncommons.file_utils import FileUtils, JsonFileUtils
from pythoncommons.logging_setup import SimpleLoggingSetup
from pythoncommons.os_utils import OsUtils
from pythoncommons.pickle_utils import PickleUtils
from pythoncommons.project_utils import ProjectUtils
from pythoncommons.string_utils import auto_str

from yarndevtools.argparser import CommandType, JenkinsTestReporterMode, JENKINS_BUILDS_EXAMINE_UNLIMITIED_VAL
from yarndevtools.common.shared_command_utils import FullEmailConfig
import urllib.request
from urllib.error import HTTPError

from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME

LOG = logging.getLogger(__name__)
EMAIL_SUBJECT_PREFIX = "YARN Daily unit test report:"
PICKLED_DATA_FILENAME = "pickled_unit_test_reporter_data.obj"
SECONDS_PER_DAY = 86400
DEFAULT_REQUEST_LIMIT = 999


@dataclass
class PickledData:
    project_name: str


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
    def __init__(self, failed_build_data: List[Tuple[str, int]]):
        self.all_builds = len(failed_build_data)
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
    def __init__(self, job_build_datas, all_failing_tests, total_no_of_builds: int):
        self.job_build_datas: List[JobBuildData] = job_build_datas
        self.jobs_by_url: Dict[str, JobBuildData] = {jbd.build_url: jbd for jbd in job_build_datas}
        self.all_failing_tests: Dict[str, int] = all_failing_tests
        self.total_no_of_builds: int = total_no_of_builds

    @property
    def known_build_urls(self):
        return self.jobs_by_url.keys()

    def convert_to_text(self, build_data_idx=-1):
        if build_data_idx > -1:
            return self.job_build_datas[build_data_idx].__str__()

    def is_valid_build(self, build_data_idx=-1):
        if build_data_idx > -1:
            return not self.job_build_datas[build_data_idx].empty_or_not_found

    def get_build_url(self, build_data_idx):
        return self.job_build_datas[build_data_idx].build_url

    def is_mail_sent(self, job_url):
        job_data = self.jobs_by_url[job_url]
        return job_data.mail_sent

    def get_sent_date(self, job_url):
        return self.jobs_by_url[job_url].sent_date

    def are_all_mail_sent(self):
        return all(job_data.mail_sent for job_data in self.jobs_by_url.values())

    def mark_sent(self, job_url):
        job_data = self.jobs_by_url[job_url]
        job_data.sent_date = DateUtils.get_current_datetime()
        job_data.mail_sent = True

    def determine_actual_number_of_builds(self, num_builds_per_config):
        build_data_count = len(self.job_build_datas)
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


class JobBuildData:
    def __init__(self, build_number, build_url, counters, testcases, empty_or_not_found=False):
        self.build_number = build_number
        self.build_url = build_url
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


class JenkinsTestReporterConfig:
    def __init__(self, output_dir: str, args):
        self.args = args
        self.request_limit = args.req_limit if hasattr(args, "req_limit") and args.req_limit else 1
        self.full_email_conf: FullEmailConfig = FullEmailConfig(args)
        self.jenkins_mode: JenkinsTestReporterMode = (
            JenkinsTestReporterMode[args.jenkins_mode.upper()]
            if hasattr(args, "jenkins_mode") and args.jenkins_mode
            else None
        )
        self.jenkins_url = args.jenkins_url
        self.job_names: List[str] = args.job_names.split(",")
        self.num_builds: int = self._determine_number_of_builds_to_examine(args.num_builds, self.request_limit)
        tc_filters_raw = args.tc_filters if hasattr(args, "tc_filters") and args.tc_filters else []
        self.tc_filters: List[TestcaseFilter] = [TestcaseFilter(*tcf.split(":")) for tcf in tc_filters_raw]
        self.enable_file_cache: bool = not args.disable_file_cache
        self.output_dir = ProjectUtils.get_session_dir_under_child_dir(FileUtils.basename(output_dir))
        self.full_cmd: str = OsUtils.determine_full_command_filtered(filter_password=True)
        self.force_download_mode = args.force_download_mode if hasattr(args, "force_download_mode") else False
        skip_email = args.skip_email if hasattr(args, "skip_email") else False
        self.force_send_email = args.force_send_email if hasattr(args, "force_send_email") else False
        self.send_mail: bool = not skip_email or not self.force_send_email
        self.omit_job_summary: bool = args.omit_job_summary if hasattr(args, "omit_job_summary") else False
        self.download_uncached_job_data: bool = (
            args.download_uncached_job_data if hasattr(args, "download_uncached_job_data") else False
        )

        # Validation
        if not self.tc_filters:
            LOG.warning("TESTCASE FILTER IS NOT SET!")
        if self.jenkins_mode and (self.jenkins_url or self.job_names):
            LOG.warning(
                "Jenkins mode is set to %s. \n"
                "Specified values for jenkins URL: %s\n"
                "Specified values for job names: %s\n"
                "Jenkins mode will take precedence!",
                self.jenkins_mode,
                self.jenkins_url,
                self.job_names,
            )
            self.jenkins_url = self.jenkins_mode.jenkins_base_url
            self.job_names = self.jenkins_mode.job_names

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
            f"Jenkins URL: {self.jenkins_url}\n"
            f"Jenkins job names: {self.job_names}\n"
            f"Number of builds to check: {self.num_builds}\n"
            f"Testcase filters: {self.tc_filters}\n"
            f"Force download mode: {self.force_download_mode}\n"
            f"Force email send mode: {self.force_send_email}\n"
        )


class JenkinsTestReporter:
    def __init__(self, args, output_dir):
        self.config = JenkinsTestReporterConfig(output_dir, args)
        self.reports: Dict[str, JenkinsJobReport] = {}  # key is the Jenkins job name

    def run(self):
        LOG.info("Starting Jenkins test reporter. " "Details: \n" f"{str(self.config)}")
        self.main()

    def _get_latest_report(self, job_name):
        return self.reports[job_name]

    def get_failed_tests(self, job_name) -> List[str]:
        latest_report = self._get_latest_report(job_name)
        if not latest_report:
            raise ValueError("Report is not queried yet or it is None!")
        return list(latest_report.all_failing_tests.keys())

    def get_num_build_data(self, job_name):
        return len(self._get_latest_report(job_name).job_build_datas)

    @property
    def testcase_filters(self) -> List[str]:
        return [tcf.as_filter_spec for tcf in self.config.tc_filters]

    @property
    def pickled_data_file_path(self):
        # TODO Utilize pythoncommons ProjectUtils
        cwd = os.getcwd()
        cached_data_dir = os.path.join(cwd, "workdir", "cached_data")
        if not os.path.exists(cached_data_dir):
            os.makedirs(cached_data_dir)
        return FileUtils.join_path(cached_data_dir, PICKLED_DATA_FILENAME)

    def load_pickled_data(self):
        LOG.info("Trying to load pickled data from file: %s", self.pickled_data_file_path)
        if FileUtils.does_file_exist(self.pickled_data_file_path):
            self.reports = PickleUtils.load(self.pickled_data_file_path)
            LOG.info("Printing email send status for jobs and builds...")
            for job_name, jenkins_job_report in self.reports.items():
                for job_url, job_build_data in jenkins_job_report.jobs_by_url.items():
                    LOG.info("Job URL: %s, email sent: %s", job_url, job_build_data.mail_sent)
            return True
        else:
            LOG.info("Pickled data file not found under path: %s", self.pickled_data_file_path)
            return False

    def pickle_report_data(self, log: bool = False):
        if log:
            LOG.debug("Final cached data object: %s", self.reports)
        LOG.info("Dumping %s object to file %s", JenkinsJobReport.__name__, self.pickled_data_file_path)
        PickleUtils.dump(self.reports, self.pickled_data_file_path)

    def get_all_filtered_testcases_from_build(self, build_data_idx: int, job_name: str):
        return [
            tc
            for filtered_res in self._get_latest_report(job_name).job_build_datas[build_data_idx].filtered_testcases
            for tc in filtered_res.testcases
        ]

    def get_filtered_testcases_from_build(self, build_data_idx: int, package: str, job_name: str):
        return [
            tc
            for filtered_res in self._get_latest_report(job_name).job_build_datas[build_data_idx].filtered_testcases
            for tc in filtered_res.testcases
            if package in tc
        ]

    def main(self):
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
            loaded = self.load_pickled_data()
            if loaded:
                LOG.info("Loaded pickled data from: %s", self.pickled_data_file_path)
        self.do_fetch()

    def do_fetch(self):
        for job_name in self.config.job_names:
            report = self._find_flaky_tests(job_name)
            self.reports[job_name] = report
            self._process_build_report(report, fail_on_empty_report=False)
        self.pickle_report_data()

    def _process_build_report(self, report, fail_on_empty_report: bool = True):
        LOG.info(f"Report list contains build results: {[bdata.build_url for bdata in report.job_build_datas]}")
        actual_num_builds = report.determine_actual_number_of_builds(report, self.config.num_builds)
        LOG.info(f"Processing {actual_num_builds} in Report...")
        if not self.config.send_mail:
            LOG.info("Skip sending email, as per configuration.")

        # TODO Remove indexing logic
        build_idx = 0
        while build_idx < actual_num_builds:
            build_url: str = report.get_build_url(build_idx)
            LOG.info(f"Processing report of build: {build_url}")
            if (
                fail_on_empty_report
                and len(report.all_failing_tests) == 0
                and report.is_valid_build(build_data_idx=build_idx)
            ):
                LOG.info(
                    f"Report with URL {build_url} is valid but does not contain any failed tests. "
                    f"Won't process further, exiting..."
                )
                # TODO We don't want to exit here in case of multile reports!
                raise SystemExit(0)

            # At this point it's certain that we have some failed tests or the build itself is invalid
            LOG.info(f"Report of build {build_url} is not valid or contains failed tests!")
            if not self.config.omit_job_summary and report.is_valid_build(build_idx):
                # TODO Move this to method of Report
                LOG.info(
                    f"\nAmong {report.total_no_of_builds} runs examined, all failed tests <#failedRuns: testName>:"
                )
                # Print summary section: all failed tests sorted by how many times they failed
                LOG.info("TESTCASE SUMMARY:")
                for tn in sorted(report.all_failing_tests, key=report.all_failing_tests.get, reverse=True):
                    LOG.info(f"{report.all_failing_tests[tn]}: {tn}")
            report_text = report.convert_to_text(build_data_idx=build_idx)
            if self.config.send_mail:
                if not report.is_mail_sent(build_url) or self.config.force_send_email:
                    self.send_mail(build_idx, report, report_text, build_url)
                    report.mark_sent(build_url)
                else:
                    LOG.info(
                        "Not sending report of job URL %s, as it was already sent before on %s.",
                        build_url,
                        report.get_sent_date(build_url),
                    )
            build_idx += 1
            if build_idx == actual_num_builds:
                self.pickle_report_data(log=True)
            else:
                self.pickle_report_data(log=False)

    # TODO move to pythoncommons but debug this before.
    @staticmethod
    def load_url_data(url):
        """ Load data from specified url """
        ourl = urllib.request.urlopen(url)
        codec = ourl.info().get_param("charset")
        content = ourl.read().decode(codec)
        return json.loads(content, strict=False)

    def list_builds(self, job_name: str):
        """ List all builds of the target project. """
        LOG.info(f"Fetching builds from Jenkins in url: {self.config.jenkins_url}/job/{job_name}")
        url = self.get_jenkins_list_builds_url(job_name)
        try:
            return self.load_url_data(url)["builds"]
        except Exception:
            LOG.error(f"Could not fetch: {url}")
            raise

    def get_jenkins_list_builds_url(self, job_name: str) -> str:
        jenkins_url = self.config.jenkins_url
        if jenkins_url.endswith("/"):
            jenkins_url = jenkins_url[:-1]
        return f"{jenkins_url}/job/{job_name}/api/json?tree=builds[url,result,timestamp]"

    @staticmethod
    def get_file_name_for_report(build_number, job_name: str):
        # TODO utilize pythoncommon ProjectUtils to get output dir
        cwd = os.getcwd()
        job_filename = job_name.replace(".", "_")
        job_dir_path = os.path.join(cwd, "workdir", "reports", job_filename)
        if not os.path.exists(job_dir_path):
            os.makedirs(job_dir_path)

        return os.path.join(job_dir_path, f"{build_number}-testreport.json")

    # TODO move to pythoncommons
    def download_test_report(self, test_report_api_json, target_file_path):
        LOG.info(
            f"Loading test report from URL: {test_report_api_json}. Download progress: {self.download_progress.short_str()}"
        )
        try:
            data = self.load_url_data(test_report_api_json)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                LOG.error(f"Test report cannot be found for build URL (HTTP 404): {test_report_api_json}")
                return {}
            else:
                raise e

        if target_file_path:
            LOG.info(f"Saving test report response JSON to cache: {target_file_path}")
            JsonFileUtils.write_data_to_file_as_json(target_file_path, data)

        return data

    def find_failing_tests(self, test_report_api_json, job_console_output, build_url, build_number, job_name: str):
        """ Find the names of any tests which failed in the given build output URL. """
        try:
            data, loaded_from_cache = self.gather_report_data_for_build(build_number, test_report_api_json, job_name)
        except Exception:
            traceback.print_exc()
            LOG.error(f"Could not open test report, check {job_console_output} for reason why it was reported failed")
            return JobBuildData(build_number, build_url, None, set()), False
        if not data or len(data) == 0:
            return JobBuildData(build_number, build_url, None, [], empty_or_not_found=True), loaded_from_cache

        return self.parse_job_data(data, build_url, build_number, job_console_output), loaded_from_cache

    def gather_report_data_for_build(self, build_number, test_report_api_json, job_name: str):
        if self.config.enable_file_cache:
            found_in_cache, target_file_path = self._is_build_data_downloaded(job_name, build_number)
            if found_in_cache:
                LOG.info(f"Loading cached test report from file: {target_file_path}")
                data = JsonFileUtils.load_data_from_json_file(target_file_path)
                return data, True
            else:
                data = self.download_test_report(test_report_api_json, target_file_path)
                return data, False
        else:
            data = self.download_test_report(test_report_api_json, None)
            return data, False

    def _is_build_data_downloaded(self, job_name, build_number):
        target_file_path = self.get_file_name_for_report(build_number, job_name)
        if os.path.exists(target_file_path):
            LOG.debug("Build found in cache. Job name: %s, Build number: %s", job_name, build_number)
            return True, target_file_path
        return False, target_file_path

    @staticmethod
    def parse_job_data(data, build_url, build_number, job_console_output_url):
        failed_testcases = set()
        for suite in data["suites"]:
            for case in suite["cases"]:
                status = case["status"]
                err_details = case["errorDetails"]
                if status == "REGRESSION" or status == "FAILED" or (err_details is not None):
                    failed_testcases.add(f"{case['className']}.{case['name']}")
        if len(failed_testcases) == 0:
            LOG.info(f"No failed tests in test report, check {job_console_output_url} for why it was reported failed.")
            return JobBuildData(build_number, build_url, None, failed_testcases)
        else:
            counters = JobBuildDataCounters(data["failCount"], data["passCount"], data["skipCount"])
            return JobBuildData(build_number, build_url, counters, failed_testcases)

    def _find_flaky_tests(self, job_name: str):
        """ Iterate runs of specified job within num_builds and collect results """
        builds = self.list_builds(job_name)
        total_no_of_builds = len(builds)
        last_n_builds = self._filter_builds_last_n_days(builds, days=self.config.num_builds)
        failed_build_data: List[Tuple[str, int]] = self._get_failed_build_urls(last_n_builds)
        failed_build_data = sorted(failed_build_data, key=lambda tup: tup[1], reverse=True)
        LOG.info(
            f"There are {len(failed_build_data)} builds "
            f"(out of {total_no_of_builds}) that have failed tests "
            f"in the past {self.config.num_builds} days. "
            f"Listing builds: {failed_build_data}"
        )

        job_datas: List[JobBuildData] = []
        tc_to_fail_count: Dict[str, int] = {}
        sent_requests: int = 0
        self.download_progress = DownloadProgress(failed_build_data)
        for i, failed_build_with_time in enumerate(failed_build_data):
            if sent_requests >= self.config.request_limit:
                LOG.error(f"Reached request limit: {sent_requests}")
                break
            failed_build_url = failed_build_with_time[0]

            # Example URL: http://build.infra.cloudera.com/job/Mawo-UT-hadoop-CDPD-7.x/191/
            # TODO Introduce class: JenkinsUrls
            build_number = failed_build_url.rsplit("/")[-2]
            job_console_output = failed_build_url + "Console"
            test_report = failed_build_url + "testReport"
            test_report_api_json = test_report + "/api/json"
            test_report_api_json += "?pretty=true"

            download_build = False
            job_added_from_cache = False
            if self.config.download_uncached_job_data and not self._is_build_data_downloaded(job_name, build_number)[0]:
                download_build = True

            # Try to get build data from cache, if found, jump to next build URL
            if self._should_load_build_data_from_cache(failed_build_url, job_name):
                LOG.info("Found build in cache, skipping: %s", failed_build_url)
                job_data = self.reports[job_name].jobs_by_url[failed_build_url]
                job_datas.append(job_data)
                self._create_testcase_to_fail_count_dict(job_data, tc_to_fail_count)
                job_added_from_cache = True

            # We would like to download job data if:
            # 1. job is not found in cache and config.download_uncached_job_data is True OR
            # 2. when job data is not found in file cache.
            if download_build or not job_added_from_cache:
                # TODO Use pythoncommons for date formatting
                timestamp = float(failed_build_with_time[1]) / 1000.0
                st = datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
                LOG.info(f"===>{test_report} ({st})")

                job_data, loaded_from_cache = self.find_failing_tests(
                    test_report_api_json, job_console_output, failed_build_url, build_number, job_name
                )

                if not job_added_from_cache:
                    job_data.filter_testcases(self.config.tc_filters)
                    job_datas.append(job_data)
                    self._create_testcase_to_fail_count_dict(job_data, tc_to_fail_count)
                self.download_progress.process_next_build()
                if not loaded_from_cache:
                    sent_requests += 1

        return JenkinsJobReport(job_datas, tc_to_fail_count, total_no_of_builds)

    def _should_load_build_data_from_cache(self, failed_build_url, job_name):
        return (
            not self.config.force_download_mode
            and job_name in self.reports
            and failed_build_url in self.reports[job_name].known_build_urls
        )

    @staticmethod
    def _create_testcase_to_fail_count_dict(job_data, tc_to_fail_count):
        if job_data.has_failed_testcases():
            for failed_testcase in job_data.testcases:
                LOG.info(f"Failed test: {failed_testcase}")
                tc_to_fail_count[failed_testcase] = tc_to_fail_count.get(failed_testcase, 0) + 1

    @staticmethod
    def _get_failed_build_urls(builds):
        return [(b["url"], b["timestamp"]) for b in builds if (b["result"] in ("UNSTABLE", "FAILURE"))]

    @staticmethod
    def _filter_builds_last_n_days(builds, days):
        # Select only those in the last N days
        min_time = int(time.time()) - SECONDS_PER_DAY * days
        return [b for b in builds if (int(b["timestamp"]) / 1000) > min_time]

    def send_mail(self, build_idx, report, report_text, report_url):
        email_subject = self._get_email_subject(build_idx, report)
        if not self.config.omit_job_summary:
            LOG.info(f"\nPRINTING REPORT: \n\n{report_text}")
        # TODO Add MailSendProgress class to track how many emails were sent
        LOG.info("Sending report in email (Job URL: %s)", report_url)
        email_service = EmailService(self.config.full_email_conf.email_conf)
        email_service.send_mail(
            self.config.full_email_conf.sender,
            email_subject,
            report_text,
            self.config.full_email_conf.recipients,
            body_mimetype=EmailMimeType.PLAIN,
        )
        LOG.info("Finished sending email to recipients")

    @staticmethod
    def _get_email_subject(build_idx, report):
        build_url = report.get_build_url(build_data_idx=build_idx)
        if report.is_valid_build(build_data_idx=build_idx):
            email_subject = f"{EMAIL_SUBJECT_PREFIX} Failed tests with build: {build_url}"
        else:
            email_subject = f"{EMAIL_SUBJECT_PREFIX} Failed to fetch test report, build is invalid: {build_url}"
        return email_subject
