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

from pythoncommons.email import EmailService, EmailMimeType
from pythoncommons.file_utils import FileUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils

from yarndevtools.common.shared_command_utils import FullEmailConfig
import urllib.request
from urllib.error import HTTPError

LOG = logging.getLogger(__name__)
EMAIL_SUBJECT_PREFIX = "YARN Daily unit test report:"
SECONDS_PER_DAY = 86400


@dataclass
class TestcaseFilter:
    project_name: str
    filter_expr: str

    @property
    def as_filter_spec(self):
        return f"{self.project_name}:{self.filter_expr}"


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


class Report:
    def __init__(self, job_build_datas, all_failing_tests):
        self.job_build_datas: List[JobBuildData] = job_build_datas
        self.all_failing_tests: Dict[str, int] = all_failing_tests

    def convert_to_text(self, build_data_idx=-1):
        if build_data_idx > -1:
            return self.job_build_datas[build_data_idx].__str__()

    def is_valid_build(self, build_data_idx=-1):
        if build_data_idx > -1:
            return not self.job_build_datas[build_data_idx].empty_or_not_found

    def get_build_url(self, build_data_idx):
        return self.job_build_datas[build_data_idx].build_url


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


def configure_logging():
    # TODO Migrate to Setup.init_logger
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)
    # set up logger to write to stdout
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    logger = logging.getLogger()
    logger.removeHandler(logger.handlers[0])
    # TODO sort this out later, it added two handlers to RootLogger so all messages were duped to stdout
    # logger.addHandler(sh)


class JenkinsTestReporterConfig:
    def __init__(self, output_dir: str, args):
        self.request_limit = args.req_limit if hasattr(args, "req_limit") and args.req_limit else 1
        self.full_email_conf: FullEmailConfig = FullEmailConfig(args)
        self.jenkins_url = args.jenkins_url
        self.job_name = args.job_name
        self.num_prev_days = args.num_prev_days
        tc_filters_raw = args.tc_filters if hasattr(args, "tc_filters") and args.tc_filters else []
        self.tc_filters: List[TestcaseFilter] = [TestcaseFilter(*tcf.split(":")) for tcf in tc_filters_raw]
        if not self.tc_filters:
            LOG.warning("TESTCASE FILTER IS NOT SET!")

        self.send_mail: bool = not args.skip_mail
        self.enable_file_cache: bool = not args.disable_file_cache
        self.output_dir = ProjectUtils.get_session_dir_under_child_dir(FileUtils.basename(output_dir))
        self.full_cmd: str = OsUtils.determine_full_command_filtered(filter_password=True)

    def __str__(self):
        return (
            f"Full command was: {self.full_cmd}\n"
            f"Jenkins URL: {self.jenkins_url}\n"
            f"Jenkins job name: {self.job_name}\n"
            f"Number of days to check: {self.num_prev_days}\n"
            f"Testcase filters: {self.tc_filters}\n"
        )


class JenkinsTestReporter:
    def __init__(self, args, output_dir):
        self.config = JenkinsTestReporterConfig(output_dir, args)
        self.report: Report or None = None
        self.report_text: str or None = None

    def run(self):
        LOG.info("Starting Jenkins test reporter. " "Details: \n" f"{str(self.config)}")
        self.main()

    @property
    def failed_tests(self) -> List[str]:
        if not self.report:
            raise ValueError("Report is not queried yet or it is None!")
        return list(self.report.all_failing_tests.keys())

    @property
    def num_build_data(self):
        return len(self.report.job_build_datas)

    @property
    def testcase_filters(self) -> List[str]:
        return [tcf.as_filter_spec for tcf in self.config.tc_filters]

    def get_all_filtered_testcases_from_build(self, build_data_idx: int):
        return [
            tc
            for filtered_res in self.report.job_build_datas[build_data_idx].filtered_testcases
            for tc in filtered_res.testcases
        ]

    def get_filtered_testcases_from_build(self, build_data_idx: int, package: str):
        return [
            tc
            for filtered_res in self.report.job_build_datas[build_data_idx].filtered_testcases
            for tc in filtered_res.testcases
            if package in tc
        ]

    def main(self):
        configure_logging()
        LOG.info(f"****Recently FAILED builds in url: {self.config.jenkins_url}/job/{self.config.job_name}")
        self.report = self.find_flaky_tests()
        self._process_build_reports(fail_on_empty_report=False)

    def _process_build_reports(self, fail_on_empty_report: bool = True):
        LOG.info(f"Report list contains build results: {[bdata.build_url for bdata in self.report.job_build_datas]}")
        builds_reports_to_process: int = min(self.config.num_prev_days, self.config.request_limit)
        LOG.info(f"Processing {builds_reports_to_process} build reports...")
        if not self.config.send_mail:
            LOG.info("Skip sending email, as per configuration.")

        build_idx = 0
        while build_idx < builds_reports_to_process:
            report_url: str = self.report.get_build_url(build_idx)
            LOG.info(f"Processing report of build: {report_url}")
            if (
                fail_on_empty_report
                and len(self.report.all_failing_tests) == 0
                and self.report.is_valid_build(build_data_idx=build_idx)
            ):
                LOG.info(
                    f"Report with URL {report_url} is valid but does not contain any failed tests. "
                    f"Won't process further, exiting..."
                )
                raise SystemExit(0)

            # At this point it's certain that we have some failed tests or the build itself is invalid
            LOG.info(f"Report of build {report_url} is not valid or contains failed tests!")
            if self.report.is_valid_build(build_idx):
                LOG.info(f"\nAmong {self.total_no_of_builds} runs examined, all failed tests <#failedRuns: testName>:")
                # Print summary section: all failed tests sorted by how many times they failed
                LOG.info("TESTCASE SUMMARY:")
                for tn in sorted(self.report.all_failing_tests, key=self.report.all_failing_tests.get, reverse=True):
                    LOG.info(f"{self.report.all_failing_tests[tn]}: {tn}")
            self.report_text = self.report.convert_to_text(build_data_idx=build_idx)
            if self.config.send_mail:
                self.send_mail(build_idx)
            build_idx += 1

    # TODO move to pythoncommons but debug this before.
    def load_url_data(self, url):
        """ Load data from specified url """
        ourl = urllib.request.urlopen(url)
        codec = ourl.info().get_param("charset")
        content = ourl.read().decode(codec)
        return json.loads(content, strict=False)

    def list_builds(self):
        """ List all builds of the target project. """
        url = self.get_jenkins_list_builds_url()
        try:
            return self.load_url_data(url)["builds"]
        except Exception:
            LOG.error(f"Could not fetch: {url}")
            raise

    def get_jenkins_list_builds_url(self) -> str:
        jenkins_url = self.config.jenkins_url
        if jenkins_url.endswith("/"):
            jenkins_url = jenkins_url[:-1]
        return f"{jenkins_url}/job/{self.config.job_name}/api/json?tree=builds[url,result,timestamp]"

    def get_file_name_for_report(self, build_number):
        # TODO utilize pythoncommon ProjectUtils to get output dir
        cwd = os.getcwd()
        job_filename = self.config.job_name.replace(".", "_")
        job_dir_path = os.path.join(cwd, "workdir", "reports", job_filename)
        if not os.path.exists(job_dir_path):
            os.makedirs(job_dir_path)

        return os.path.join(job_dir_path, f"{build_number}-testreport.json")

    # TODO move to pythoncommons
    @staticmethod
    def write_test_report_to_file(data, target_file_path):
        with open(target_file_path, "w") as target_file:
            json.dump(data, target_file)

    # TODO move to pythoncommons
    @staticmethod
    def read_test_report_from_file(file_path):
        with open(file_path) as json_file:
            return json.load(json_file)

    # TODO move to pythoncommons
    def download_test_report(self, test_report_api_json, target_file_path):
        LOG.info(f"Loading test report from URL: {test_report_api_json}")
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
            self.write_test_report_to_file(data, target_file_path)

        return data

    def find_failing_tests(self, test_report_api_json, job_console_output, build_url, build_number):
        """ Find the names of any tests which failed in the given build output URL. """
        try:
            data = self.gather_report_data_for_build(build_number, test_report_api_json)
        except Exception:
            traceback.print_exc()
            LOG.error(f"Could not open test report, check {job_console_output} for reason why it was reported failed")
            return JobBuildData(build_number, build_url, None, set())
        if not data or len(data) == 0:
            return JobBuildData(build_number, build_url, None, [], empty_or_not_found=True)

        return self.parse_job_data(data, build_url, build_number, job_console_output)

    def gather_report_data_for_build(self, build_number, test_report_api_json):
        if self.config.enable_file_cache:
            target_file_path = self.get_file_name_for_report(build_number)
            if os.path.exists(target_file_path):
                LOG.info(f"Loading cached test report from file: {target_file_path}")
                data = self.read_test_report_from_file(target_file_path)
            else:
                data = self.download_test_report(test_report_api_json, target_file_path)
        else:
            data = self.download_test_report(test_report_api_json, None)
        return data

    def parse_job_data(self, data, build_url, build_number, job_console_output_url):
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

    def find_flaky_tests(self):
        """ Iterate runs of specified job within num_prev_days and collect results """
        # First list all builds
        builds = self.list_builds()
        self.total_no_of_builds = len(builds)
        last_n_builds = self._filter_builds_last_n_days(builds, days=self.config.num_prev_days)
        failed_build_data: List[Tuple[str, int]] = self._get_failed_build_urls(last_n_builds)
        failed_build_data = sorted(failed_build_data, key=lambda tup: tup[1], reverse=True)
        LOG.info(
            f"There are {len(failed_build_data)} builds "
            f"(out of {self.total_no_of_builds}) that have failed tests "
            f"in the past {self.config.num_prev_days} days. "
            f"Listing builds: {failed_build_data}"
        )

        job_datas: List[JobBuildData] = []
        tc_to_fail_count: Dict[str, int] = {}
        for i, failed_build_with_time in enumerate(failed_build_data):
            if i >= self.config.request_limit:
                LOG.error(f"Reached request limit: {i}")
                break
            failed_build = failed_build_with_time[0]

            # Example URL: http://build.infra.cloudera.com/job/Mawo-UT-hadoop-CDPD-7.x/191/
            build_number = failed_build.rsplit("/")[-2]
            job_console_output = failed_build + "Console"
            test_report = failed_build + "testReport"
            test_report_api_json = test_report + "/api/json"
            test_report_api_json += "?pretty=true"

            timestamp = float(failed_build_with_time[1]) / 1000.0
            st = datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
            LOG.info(f"===>{test_report} ({st})")

            job_data: JobBuildData = self.find_failing_tests(
                test_report_api_json, job_console_output, failed_build, build_number
            )
            job_data.filter_testcases(self.config.tc_filters)
            job_datas.append(job_data)

            if job_data.has_failed_testcases():
                for failed_testcase in job_data.testcases:
                    LOG.info(f"Failed test: {failed_testcase}")
                    tc_to_fail_count[failed_testcase] = tc_to_fail_count.get(failed_testcase, 0) + 1

        return Report(job_datas, tc_to_fail_count)

    @staticmethod
    def _get_failed_build_urls(builds):
        return [(b["url"], b["timestamp"]) for b in builds if (b["result"] in ("UNSTABLE", "FAILURE"))]

    @staticmethod
    def _filter_builds_last_n_days(builds, days):
        # Select only those in the last N days
        min_time = int(time.time()) - SECONDS_PER_DAY * days
        return [b for b in builds if (int(b["timestamp"]) / 1000) > min_time]

    def send_mail(self, build_idx):
        email_subject = self._get_email_subject(build_idx, self.report)
        LOG.info(f"\nPRINTING REPORT: \n\n{self.report_text}")
        LOG.info("Sending report in email")
        email_service = EmailService(self.config.full_email_conf.email_conf)
        email_service.send_mail(
            self.config.full_email_conf.sender,
            email_subject,
            self.report_text,
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
