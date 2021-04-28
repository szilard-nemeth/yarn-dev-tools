#!/usr/local/bin/python3
import json
import os
import sys
import traceback
import datetime
import json as simplejson
import logging
import time
from typing import List, Dict

from pythoncommons.email import EmailService, EmailMimeType
from pythoncommons.file_utils import FileUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils

from yarndevtools.common.shared_command_utils import FullEmailConfig
import urllib.request

EMAIL_SUBJECT_PREFIX = "YARN Daily unit test report:"

# Configuration
SECONDS_PER_DAY = 86400

# TODO eliminate this
# total number of runs to examine
numRunsToExamine = 0

# TODO move this to config
# Whether to enable file cache for testreport JSON responses
ENABLE_FILE_CACHE = True

LOG = logging.getLogger(__name__)


class Report:
    def __init__(self, job_build_datas, all_failing_tests):
        self.job_build_datas = job_build_datas
        self.all_failing_tests: Dict[str, int] = all_failing_tests

    def convert_to_text(self, build_data_idx=-1):
        if build_data_idx > -1:
            return self.job_build_datas[build_data_idx].__str__()

    def is_valid_build(self, build_data_idx=-1):
        if build_data_idx > -1:
            return not self.job_build_datas[build_data_idx].empty_or_not_found

    def get_build_link(self, build_data_idx):
        return self.job_build_datas[build_data_idx].build_link


class JobBuildData:
    def __init__(self, build_number, build_link, counters, testcases, empty_or_not_found=False):
        self.build_number = build_number
        self.build_link = build_link
        self.counters = counters
        self.testcases = testcases
        self.tc_filter = None
        self.filtered_testcases = None
        self.no_of_failed_filtered_tc = None
        self.empty_or_not_found = empty_or_not_found

    def has_failed_testcases(self):
        return len(self.testcases) > 0

    def filter_testcases(self, tc_filter):
        self.tc_filter = tc_filter
        if tc_filter:
            self.filtered_testcases = list(filter(lambda tc: tc_filter in tc, self.testcases))
            self.no_of_failed_filtered_tc = len(self.filtered_testcases)

    def __str__(self):
        if self.empty_or_not_found:
            return self._str_empty_report()
        else:
            return self._str_normal_report()

    def _str_empty_report(self):
        return """
Build number: {build_number}
Build link: {build_link}
!!REPORT WAS NOT FOUND OR IT IS EMPTY!!
        """.format(
            build_number=self.build_number,
            build_link=self.build_link,
        )

    def _str_normal_report(self):
        filtered_testcases = ""
        if self.tc_filter and self.filter_testcases:
            filtered_testcases += "FILTER: {}\n".format(self.tc_filter)
            filtered_testcases += "Number of failed testcases (filtered): {}\n".format(len(self.filtered_testcases))
            filtered_testcases += "Failed testcases (filtered): \n {testcases}".format(
                testcases="\n".join(self.filtered_testcases)
            )
        if filtered_testcases:
            filtered_testcases = "\n" + filtered_testcases
            filtered_testcases += "\n"
        return """Counters:
{ctr}

Build number: {build_number}

Build link: {build_link}
{filtered_testcases}
Failed testcases: {testcases}
        """.format(
            ctr=self.counters,
            build_number=self.build_number,
            build_link=self.build_link,
            testcases="\n".join(self.testcases),
            filtered_testcases=filtered_testcases,
        )


class JobBuildDataCounters:
    def __init__(self, failed, passed, skipped):
        self.failed = failed
        self.passed = passed
        self.skipped = skipped

    def __str__(self):
        return "Failed: {}, Passed: {}, Skipped: {}".format(self.failed, self.passed, self.skipped)


def load_url_data(url):
    """ Load data from specified url """
    ourl = urllib.request.urlopen(url)
    codec = ourl.info().get_param("charset")
    content = ourl.read().decode(codec)
    data = simplejson.loads(content, strict=False)
    return data


def list_builds(jenkins_url, job_name):
    """ List all builds of the target project. """
    if jenkins_url.endswith("/"):
        jenkins_url = jenkins_url[:-1]
    url = "%(jenkins)s/job/%(job_name)s/api/json?tree=builds[url,result,timestamp]" % dict(
        jenkins=jenkins_url, job_name=job_name
    )
    try:
        data = load_url_data(url)
    except Exception:
        logging.error("Could not fetch: %s" % url)
        raise
    return data["builds"]


def get_file_name_for_report(job_name, build_number):
    # TODO utilize pythoncommon ProjectUtils to get output dir
    cwd = os.getcwd()
    job_name = job_name.replace(".", "_")
    job_dir_path = os.path.join(cwd, "workdir", "reports", job_name)
    if not os.path.exists(job_dir_path):
        os.makedirs(job_dir_path)

    return os.path.join(job_dir_path, build_number + "-testreport.json")


def write_test_report_to_file(data, target_file_path):
    with open(target_file_path, "w") as target_file:
        json.dump(data, target_file)


def read_test_report_from_file(file_path):
    with open(file_path) as json_file:
        return json.load(json_file)


def download_test_report(test_report_api_json, target_file_path):
    LOG.info("Loading test report from URL: %s", test_report_api_json)
    try:
        data = load_url_data(test_report_api_json)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            logging.error("Test report cannot be found for build URL (HTTP 404): %s", test_report_api_json)
            return {}
        else:
            raise e

    if target_file_path:
        LOG.info("Saving test report response JSON to cache: %s", target_file_path)
        write_test_report_to_file(data, target_file_path)

    return data


def find_failing_tests(test_report_api_json, job_console_output, build_link, job_name, build_number):
    """ Find the names of any tests which failed in the given build output URL. """
    try:
        data = gather_report_data_for_build(build_number, job_name, test_report_api_json)
    except Exception:
        traceback.print_exc()
        logging.error("    Could not open test report, check " + job_console_output + " for why it was reported failed")
        return JobBuildData(build_number, build_link, None, set())
    if not data or len(data) == 0:
        return JobBuildData(build_number, build_link, None, [], empty_or_not_found=True)

    return parse_job_data(data, build_link, build_number, job_console_output)


def gather_report_data_for_build(build_number, job_name, test_report_api_json):
    if ENABLE_FILE_CACHE:
        target_file_path = get_file_name_for_report(job_name, build_number)
        if os.path.exists(target_file_path):
            LOG.info("Loading cached test report from file: %s", target_file_path)
            data = read_test_report_from_file(target_file_path)
        else:
            data = download_test_report(test_report_api_json, target_file_path)
    else:
        data = download_test_report(test_report_api_json, None)
    return data


def parse_job_data(data, build_link, build_number, job_console_output_url):
    failed_testcases = set()
    for suite in data["suites"]:
        for case in suite["cases"]:
            status = case["status"]
            err_details = case["errorDetails"]
            if status == "REGRESSION" or status == "FAILED" or (err_details is not None):
                failed_testcases.add(case["className"] + "." + case["name"])
    if len(failed_testcases) == 0:
        LOG.info(
            "    No failed tests in test Report, check " + job_console_output_url + " for why it was reported failed."
        )
        return JobBuildData(build_number, build_link, None, failed_testcases)
    else:
        counters = JobBuildDataCounters(data["failCount"], data["passCount"], data["skipCount"])
        return JobBuildData(build_number, build_link, counters, failed_testcases)


def find_flaky_tests(jenkins_url, job_name, num_prev_days, request_limit, tc_filter):
    """ Iterate runs of specified job within num_prev_days and collect results """
    global numRunsToExamine
    # First list all builds
    builds = list_builds(jenkins_url, job_name)

    # Select only those in the last N days
    min_time = int(time.time()) - SECONDS_PER_DAY * num_prev_days
    builds = [b for b in builds if (int(b["timestamp"]) / 1000) > min_time]

    # Filter out only those that failed
    failing_build_urls = [(b["url"], b["timestamp"]) for b in builds if (b["result"] in ("UNSTABLE", "FAILURE"))]
    failing_build_urls = sorted(failing_build_urls, key=lambda tup: tup[0], reverse=True)

    total_no_of_builds = len(builds)
    num = len(failing_build_urls)
    numRunsToExamine = total_no_of_builds
    LOG.info(
        "    THERE ARE "
        + str(num)
        + " builds (out of "
        + str(total_no_of_builds)
        + ") that have failed tests in the past "
        + str(num_prev_days)
        + " days"
        + ((".", ", as listed below:\n")[num > 0])
    )

    job_datas = []
    all_failing: Dict[str, int] = dict()
    for i, failed_build_with_time in enumerate(failing_build_urls):
        if i >= request_limit:
            break
        failed_build = failed_build_with_time[0]

        # Example URL: http://build.infra.cloudera.com/job/Mawo-UT-hadoop-CDPD-7.x/191/
        build_number = failed_build.rsplit("/")[-2]
        job_console_output = failed_build + "Console"
        test_report = failed_build + "testReport"
        test_report_api_json = test_report + "/api/json"
        test_report_api_json += "?pretty=true"

        ts = float(failed_build_with_time[1]) / 1000.0
        st = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        LOG.info("===>%s" % str(test_report) + " (" + st + ")")

        job_data = find_failing_tests(test_report_api_json, job_console_output, failed_build, job_name, build_number)
        job_data.filter_testcases(tc_filter)
        job_datas.append(job_data)

        if job_data.has_failed_testcases():
            for ftest in job_data.testcases:
                LOG.info("    Failed test: %s" % ftest)
                all_failing[ftest] = all_failing.get(ftest, 0) + 1

    return Report(job_datas, all_failing)


def configure_logging():
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)
    # set up logger to write to stdout
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    logger = logging.getLogger()
    logger.removeHandler(logger.handlers[0])
    logger.addHandler(sh)


class JenkinsTestReporterConfig:
    def __init__(self, output_dir: str, args):
        self.full_email_conf: FullEmailConfig = FullEmailConfig(args)
        self.jenkins_url = args.jenkins_url
        self.job_name = args.job_name
        self.num_prev_days = args.num_prev_days
        self.tc_filter = args.tc_filter
        self.send_mail: bool = not args.skip_mail
        self.enable_file_cache: bool = not args.disable_file_cache
        self.output_dir = ProjectUtils.get_session_dir_under_child_dir(FileUtils.basename(output_dir))
        self.full_cmd: str = OsUtils.determine_full_command_filtered(filter_password=True)

        current_module = sys.modules[__name__]
        current_module.ENABLE_FILE_CACHE = self.enable_file_cache

    def __str__(self):
        return (
            f"Full command was: {self.full_cmd}\n"
            f"Jenkins URL: {self.jenkins_url}\n"
            f"Jenkins job name: {self.job_name}\n"
            f"Number of days to check: {self.num_prev_days}\n"
            f"Testcase filter: {self.tc_filter}\n"
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
    def testcase_filter(self):
        return self.config.tc_filter

    def get_filtered_testcases_from_build(self, build_data_idx: int):
        return self.report.job_build_datas[build_data_idx].filtered_testcases

    def main(self):
        global numRunsToExamine
        configure_logging()
        LOG.info("****Recently FAILED builds in url: " + self.config.jenkins_url + "/job/" + self.config.job_name + "")
        request_limit = 1

        tc_filter = self.config.tc_filter if self.config.tc_filter else ""
        if not tc_filter:
            LOG.warning("TESTCASE FILTER IS NOT SET!")
        self.report = find_flaky_tests(
            self.config.jenkins_url, self.config.job_name, self.config.num_prev_days, request_limit, tc_filter
        )

        build_idx = 0
        if len(self.report.all_failing_tests) == 0 and self.report.is_valid_build(build_data_idx=build_idx):
            LOG.info("Report is valid and does not contain any failed tests. Won't send mail, exiting...")
            raise SystemExit(0)

        # We have some failed tests OR the build is invalid
        LOG.info("Report is not valid or contains failed tests!")

        if len(self.report.job_build_datas) > 1:
            LOG.info("Report contains more than 1 build result, using the first build result while sending the mail.")

        if self.report.is_valid_build(build_idx):
            LOG.info(
                "\nAmong " + str(numRunsToExamine) + " runs examined, all failed " + "tests <#failedRuns: testName>:"
            )

            # Print summary section: all failed tests sorted by how many times they failed
            LOG.info("TESTCASE SUMMARY:")
            for tn in sorted(self.report.all_failing_tests, key=self.report.all_failing_tests.get, reverse=True):
                LOG.info("    " + str(self.report.all_failing_tests[tn]) + ": " + tn)

        self.report_text = self.report.convert_to_text(build_data_idx=build_idx)

        # TODO idea: Attach zipped json + html jenkins report to email
        if self.config.send_mail:
            self.send_mail(build_idx)
        else:
            LOG.info("Not sending email, as per configuration.")

    def send_mail(self, build_idx):
        email_subject = self._get_email_subject(build_idx, self.report)
        LOG.info("Trying to send report in email. Report text: %s", self.report_text)
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
        build_link = report.get_build_link(build_data_idx=build_idx)
        if report.is_valid_build(build_data_idx=build_idx):
            email_subject = f"{EMAIL_SUBJECT_PREFIX} Failed tests with build: {build_link}"
        else:
            email_subject = f"{EMAIL_SUBJECT_PREFIX} Failed to fetch test report, build is invalid: {build_link}"
        return email_subject
