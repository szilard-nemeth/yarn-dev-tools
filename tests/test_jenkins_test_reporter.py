import dataclasses
import json
import logging
import random
import re
import unittest
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Dict, Tuple, Set

import httpretty as httpretty
from coolname import generate_slug
from pythoncommons.date_utils import DateUtils
from pythoncommons.project_utils import ProjectUtils

from tests.test_utilities import Object
from yarndevtools.argparser import CommandType
from yarndevtools.commands.jenkinstestreporter.jenkins_test_reporter import JenkinsTestReporter
from yarndevtools.constants import JENKINS_TEST_REPORTER, PROJECT_NAME

P1 = "org.somepackage1"
P2 = "org.somepackage2"
P3 = "org.somepackage3"
P4 = "org.somepackage4"
STDOUT = "stdout"
STDERR = "stderrr"


YARN_TC_FILTER = "YARN:org.apache.hadoop.yarn"
MAPRED_TC_FILTER = "MAPREDUCE:org.apache.hadoop.mapreduce"
MULTI_FILTER = [YARN_TC_FILTER, MAPRED_TC_FILTER]

JENKINS_MAIN_URL = "http://build.infra.cloudera.com"
MAWO_JOB_NAME_7X = "Mawo-UT-hadoop-CDPD-7.x"
MAWO_JOB_NAME_71X = "Mawo-UT-hadoop-CDPD-7.1.x"
BUILD_URL_ID_KEY = "build_id"
BUILD_URL_MAWO_7X_TEMPLATE = f"{JENKINS_MAIN_URL}/job/{MAWO_JOB_NAME_7X}/{{{BUILD_URL_ID_KEY}}}/"
BUILD_URL_MAWO_71X_TEMPLATE = f"{JENKINS_MAIN_URL}/job/{MAWO_JOB_NAME_71X}/{{{BUILD_URL_ID_KEY}}}/"

USE_REAL_API = False

LOG = logging.getLogger(__name__)
# TODO Migrate to SimpleLoggingSetup.init_logger
logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)
LOG.setLevel(logging.DEBUG)


class TestCaseStatus(Enum):
    PASSED = 0
    FAILED = 1
    SKIPPED = 2


class BuildStatus(Enum):
    SUCCESS = 0
    FAILURE = 1


@dataclass
class JenkinsTestCase:
    className: str
    name: str
    status: str
    duration: float = None
    skipped: bool = False
    stderr: str = None
    stdout: str = None
    errorDetails: str = None


@dataclass
class JenkinsTestSuite:
    cases: List[JenkinsTestCase]
    name: str
    duration: float = None
    stdout: str = None
    stderr: str = None


@dataclass
class JenkinsTestReport:
    failCount: int
    passCount: int
    skipCount: int
    suites: List[JenkinsTestSuite]
    _class: str = "hudson.tasks.junit.TestResult"
    empty: bool = False
    duration: float = None


@dataclass
class JenkinsBuild:
    result: str
    timestamp: int
    url: str
    _class: str = "hudson.model.FreeStyleBuild"


@dataclass
class JenkinsBuilds:
    builds: List[JenkinsBuild]


@dataclass
class JenkinsReportJsonSpec:
    # Key package, value: number of testcases with result type
    failed: Dict[str, int]
    passed: Dict[str, int]
    skipped: Dict[str, int]

    def __post_init__(self):
        self.failed_count = sum(self.failed.values())
        self.passed_count = sum(self.passed.values())
        self.skipped_count = sum(self.skipped.values())
        self.no_of_all_tcs: int = self.failed_count + self.passed_count + self.skipped_count

        if self.no_of_all_tcs < 5:
            raise ValueError("Minimum required value of all testcases is 5!")

        self.failed_test_classnames = [self._generate_classname() for _ in range(5)]
        self.passed_test_classnames = [self._generate_classname() for _ in range(5)]
        self.skipped_test_classnames = [self._generate_classname() for _ in range(5)]

        # Key: testcase FQN, value: status
        self.testcase_statuses: Dict[str, str] = {}

        self.testcases_by_suites: Dict[str, List[Tuple[str, TestCaseStatus]]] = {}

        self._add_to_result_dict(TestCaseStatus.FAILED)
        self._add_to_result_dict(TestCaseStatus.PASSED)
        self._add_to_result_dict(TestCaseStatus.SKIPPED)

        if len(self.testcase_statuses) != self.no_of_all_tcs:
            raise ValueError(
                "Size of dict should be equal to number of all testcases!"
                f"Size of dict: {len(self.testcase_statuses)}"
                f"All testcases: {self.no_of_all_tcs}"
            )

    def get_failed_testcases(self, package: str):
        return [
            k
            for k, v in self.testcase_statuses.items()
            if k.startswith(package) and v == TestCaseStatus.FAILED.name.upper()
        ]

    def get_all_failed_testcases(self):
        return [k for k, v in self.testcase_statuses.items() if v == TestCaseStatus.FAILED.name.upper()]

    @staticmethod
    def _generate_classname(words=3):
        gen = generate_slug(words)
        comps = gen.split("-")
        return "".join([f"{c[0].upper()}{c[1:]}" for c in comps])

    def _add_to_result_dict(self, status: TestCaseStatus):
        if status == TestCaseStatus.FAILED:
            package_to_count, classnames = self.failed, self.failed_test_classnames
        elif status == TestCaseStatus.PASSED:
            package_to_count, classnames = self.passed, self.passed_test_classnames
        elif status == TestCaseStatus.SKIPPED:
            package_to_count, classnames = self.skipped, self.skipped_test_classnames
        else:
            raise ValueError("Unknown test case status: " + str(status))

        for package, no_of_tcs in package_to_count.items():
            for idx in range(0, no_of_tcs):
                tc_name = f"tc-{generate_slug(2)}"
                class_name = classnames[idx % 5]
                class_name_fqn: str = f"{package}.{class_name}"
                tc_fqn: str = f"{class_name_fqn}.{tc_name}"
                self.testcase_statuses[tc_fqn] = status.name.upper()
                if class_name_fqn not in self.testcases_by_suites:
                    self.testcases_by_suites[class_name_fqn] = []
                self.testcases_by_suites[class_name_fqn].append((tc_name, status))


class JenkinsReportGenerator:
    @staticmethod
    def generate(spec: JenkinsReportJsonSpec) -> JenkinsTestReport:
        suites: List[JenkinsTestSuite] = []
        for suite, tc_tuples in spec.testcases_by_suites.items():
            testcases: List[JenkinsTestCase] = []
            for tc_name, tc_status in tc_tuples:
                stdout = f"{tc_name}::{STDOUT}"
                stderr = f"{tc_name}::{STDERR}"
                duration: float = random.uniform(1.5, 10.0) * 10
                skipped = True if tc_status == TestCaseStatus.SKIPPED else False
                testcases.append(
                    JenkinsTestCase(
                        suite,
                        tc_name,
                        tc_status.name.upper(),
                        skipped=skipped,
                        duration=duration,
                        stdout=stdout,
                        stderr=stderr,
                    )
                )

            stdout_suite = f"{suite}::{STDOUT}"
            stderr_suite = f"{suite}::{STDERR}"
            duration_suite: float = random.uniform(1.5, 10.0) * 10
            suites.append(
                JenkinsTestSuite(testcases, suite, duration=duration_suite, stdout=stdout_suite, stderr=stderr_suite)
            )

        return JenkinsTestReport(spec.failed_count, spec.passed_count, spec.skipped_count, suites, duration=300.0)


class JenkinsBuildsGenerator:
    @staticmethod
    def generate(build_url_template: str, num_builds: int = 51, latest_build_num: int = 215) -> JenkinsBuilds:
        if BUILD_URL_ID_KEY not in build_url_template:
            raise ValueError(
                "Should have received a build URL template that contains placeholder: " "{" + BUILD_URL_ID_KEY + "}'"
            )

        now: datetime = DateUtils.now()
        builds: List[JenkinsBuild] = []
        for i in range(num_builds):
            build_url = build_url_template.format(**{BUILD_URL_ID_KEY: latest_build_num - i})
            # Jenkins uses milliseconds as timestamp value
            timestamp: int = DateUtils.datetime_minus(now, days=i).timestamp() * 1000
            build = JenkinsBuild(BuildStatus.FAILURE.name, timestamp, build_url)
            builds.append(build)

        return JenkinsBuilds(builds)


class TestJenkinsTestReporter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Invoke this to setup main output directory and avoid test failures while initing config
        cls.project_out_root = ProjectUtils.get_test_output_basedir(PROJECT_NAME)
        ProjectUtils.get_test_output_child_dir(JENKINS_TEST_REPORTER)

    def setUp(self):
        if not USE_REAL_API:
            # enable HTTPretty so that it will monkey patch the socket module
            httpretty.enable()

    def tearDown(self) -> None:
        if not USE_REAL_API:
            # disable afterwards, so that you will have no problems in code that uses that socket module
            httpretty.disable()
            # reset HTTPretty state (clean up registered urls and request history)
            httpretty.reset()

    @staticmethod
    def generate_args(
        tc_filters: List[str] = None,
        job_name: str = MAWO_JOB_NAME_7X,
        jenkins_url: str = JENKINS_MAIN_URL,
        num_prev_days: int = 14,
    ):
        if not tc_filters:
            tc_filters = [YARN_TC_FILTER]
        args = Object()
        args.account_user = "test_user@gmail.com"
        args.account_password = "dummy"
        args.smtp_port = "465"
        args.smtp_server = "smtp.gmail.com"
        args.recipients = ["test@recipient.com"]
        args.sender = "Jenkins test reporter"
        args.jenkins_url = jenkins_url
        args.job_name = job_name
        args.num_prev_days = num_prev_days
        args.tc_filters = tc_filters
        args.skip_mail = True
        args.disable_file_cache = True
        args.debug = True
        args.verbose = True
        args.command = CommandType.JENKINS_TEST_REPORTER.real_name
        return args

    @property
    def output_dir(self):
        return ProjectUtils.get_test_output_child_dir(JENKINS_TEST_REPORTER)

    @staticmethod
    def _get_jenkins_report_as_json(spec):
        report: JenkinsTestReport = JenkinsReportGenerator.generate(spec)
        report_as_dict = dataclasses.asdict(report)
        report_json = json.dumps(report_as_dict, indent=4)
        return report_json

    @staticmethod
    def _get_default_jenkins_builds_as_json(build_id=200):
        builds: JenkinsBuilds = JenkinsBuildsGenerator.generate(BUILD_URL_MAWO_7X_TEMPLATE, latest_build_num=build_id)
        builds_as_dict = dataclasses.asdict(builds)
        builds_json = json.dumps(builds_as_dict, indent=4)
        return build_id, builds_json

    @staticmethod
    def _mock_jenkins_report_api(report_json, jenkins_url=JENKINS_MAIN_URL, job_name=MAWO_JOB_NAME_7X, build_id=200):
        if jenkins_url.endswith("/"):
            jenkins_url = jenkins_url[:-1]
        httpretty.register_uri(
            httpretty.GET,
            re.compile(rf"{jenkins_url}/job/{job_name}/{build_id}/testReport/api/json.*"),
            body=report_json,
        )

    @staticmethod
    def _mock_jenkins_build_api(
        builds_json,
        jenkins_url=JENKINS_MAIN_URL,
        job_name=MAWO_JOB_NAME_7X,
    ):
        if jenkins_url.endswith("/"):
            jenkins_url = jenkins_url[:-1]
        httpretty.register_uri(
            httpretty.GET,
            re.compile(rf"{jenkins_url}/job/{job_name}/api/json.*"),
            body=builds_json,
        )

    def _assert_all_failed_testcases(self, reporter, spec, expected_failed_count=-1):
        all_failed_tests_in_jenkins_report: Set[str] = set(spec.get_all_failed_testcases())
        failed_tests: Set[str] = set(reporter.failed_tests)
        self.assertEqual(expected_failed_count, len(failed_tests))
        self.assertEqual(expected_failed_count, len(all_failed_tests_in_jenkins_report))
        self.assertSetEqual(failed_tests, all_failed_tests_in_jenkins_report)

    def _assert_num_filtered_testcases_single_build(
        self,
        reporter,
        filters: List[str] = None,
        expected_num_build_data=-1,
        expected_failed_testcases_dict: Dict[str, List[str]] = None,
    ):
        if not expected_failed_testcases_dict:
            expected_failed_testcases_dict = {}
        if not filters:
            filters = [YARN_TC_FILTER]
        # Sanity check
        for f in filters:
            if f not in expected_failed_testcases_dict.keys():
                raise ValueError(
                    "Found filter that is not addded to expected_failed_testcases!"
                    f"Filter: {f}"
                    f"Expected failed testcases dict: {expected_failed_testcases_dict}"
                )

        self.assertEqual(filters, reporter.testcase_filters)
        self.assertEqual(expected_num_build_data, reporter.num_build_data)

        for tc_filter in filters:
            package = self._get_package_from_filter(tc_filter)
            actual_failed_testcases = reporter.get_filtered_testcases_from_build(0, package)
            expected_failed_testcases: List[str] = expected_failed_testcases_dict[tc_filter]
            self.assertEqual(len(expected_failed_testcases), len(actual_failed_testcases))
            self.assertListEqual(sorted(actual_failed_testcases), sorted(expected_failed_testcases))

    def test_successful_api_response_verify_failed_testcases(self):
        spec = JenkinsReportJsonSpec(
            failed={P3: 10, P4: 20},
            skipped={P1: 10, P2: 20},
            passed={P1: 10, P2: 20},
        )
        build_id, builds_json = self._get_default_jenkins_builds_as_json(build_id=200)
        report_json = self._get_jenkins_report_as_json(spec)
        self._mock_jenkins_build_api(builds_json)
        self._mock_jenkins_report_api(report_json, build_id=200)

        reporter = JenkinsTestReporter(self.generate_args(), self.output_dir)
        reporter.run()
        LOG.info("Report result: %s", reporter.report_text)
        self._assert_all_failed_testcases(reporter, spec, expected_failed_count=30)
        self._assert_num_filtered_testcases_single_build(
            reporter,
            filters=[YARN_TC_FILTER],
            expected_num_build_data=1,
            expected_failed_testcases_dict={YARN_TC_FILTER: []},
        )

    def test_successful_api_response_verify_filtered_testcases(self):
        spec = JenkinsReportJsonSpec(
            failed={P3: 10, P4: 20, self._get_package_from_filter(YARN_TC_FILTER): 25},
            skipped={P1: 10, P2: 20},
            passed={P1: 10, P2: 20},
        )
        failed_testcases: List[str] = spec.get_failed_testcases(self._get_package_from_filter(YARN_TC_FILTER))
        build_id, builds_json = self._get_default_jenkins_builds_as_json(build_id=200)
        report_json = self._get_jenkins_report_as_json(spec)
        self._mock_jenkins_build_api(builds_json)
        self._mock_jenkins_report_api(report_json, build_id=200)

        reporter = JenkinsTestReporter(self.generate_args(), self.output_dir)
        reporter.run()
        LOG.info("Report result: %s", reporter.report_text)
        self._assert_all_failed_testcases(reporter, spec, expected_failed_count=55)
        self._assert_num_filtered_testcases_single_build(
            reporter,
            filters=[YARN_TC_FILTER],
            expected_num_build_data=1,
            expected_failed_testcases_dict={YARN_TC_FILTER: failed_testcases},
        )

    def test_successful_api_response_verify_multi_filtered(self):
        spec = JenkinsReportJsonSpec(
            failed={
                P3: 10,
                P4: 20,
                self._get_package_from_filter(YARN_TC_FILTER): 5,
                self._get_package_from_filter(MAPRED_TC_FILTER): 10,
            },
            skipped={P1: 10, P2: 20},
            passed={P1: 10, P2: 20},
        )
        failed_yarn_testcases: List[str] = spec.get_failed_testcases(self._get_package_from_filter(YARN_TC_FILTER))
        failed_mr_testcases: List[str] = spec.get_failed_testcases(self._get_package_from_filter(MAPRED_TC_FILTER))
        build_id, builds_json = self._get_default_jenkins_builds_as_json(build_id=200)
        report_json = self._get_jenkins_report_as_json(spec)
        self._mock_jenkins_build_api(builds_json)
        self._mock_jenkins_report_api(report_json, build_id=200)

        reporter = JenkinsTestReporter(self.generate_args(tc_filters=MULTI_FILTER), self.output_dir)
        reporter.run()
        LOG.info("Report result: %s", reporter.report_text)
        self._assert_all_failed_testcases(reporter, spec, expected_failed_count=45)
        self._assert_num_filtered_testcases_single_build(
            reporter,
            filters=MULTI_FILTER,
            expected_num_build_data=1,
            expected_failed_testcases_dict={
                YARN_TC_FILTER: failed_yarn_testcases,
                MAPRED_TC_FILTER: failed_mr_testcases,
            },
        )

    @staticmethod
    def _get_package_from_filter(filter: str):
        return filter.split(":")[-1]
