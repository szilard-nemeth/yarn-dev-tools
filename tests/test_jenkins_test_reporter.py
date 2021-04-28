import dataclasses
import json
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

from test_utilities import Object
from yarndevtools.commands.jenkinstestreporter.jenkins_test_reporter import JenkinsTestReporter
from yarndevtools.constants import JENKINS_TEST_REPORTER, PROJECT_NAME

HADOOP_TC_FILTER = "org.apache.hadoop.yarn"
DEFAULT_TC_FILTER = HADOOP_TC_FILTER

BUILD_URL_ID_KEY = "build_id"
BUILD_URL_MAWO_7X_TEMPLATE = "http://build.infra.cloudera.com/job/Mawo-UT-hadoop-CDPD-7.x/{" + BUILD_URL_ID_KEY + "}/"
BUILD_URL_MAWO_71X_TEMPLATE = "http://build.infra.cloudera.com/job/Mawo-UT-hadoop-CDPD-7.1.x/{build_id}/"

USE_REAL_API = False


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
                stdout = f"{tc_name}::stdout"
                stderr = f"{tc_name}::stderr"
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

            stdout_suite = f"{suite}::stdout"
            stderr_suite = f"{suite}::stderr"
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

    # TODO test missing args data: account password, etc?
    # TODO test if jenkins is not available, i.e. won't return list of builds
    # TODO test if all builds are out of date range (last N days)

    @property
    def args(self):
        args = Object()
        args.account_user = "test_user@gmail.com"
        args.account_password = "dummy"
        args.smtp_port = "465"
        args.smtp_server = "smtp.gmail.com"
        args.recipients = ["test@recipient.com"]
        args.sender = "Jenkins test reporter"
        args.jenkins_url = "http://build.infra.cloudera.com/"
        args.job_name = "Mawo-UT-hadoop-CDPD-7.x"
        args.num_prev_days = 14
        args.tc_filter = DEFAULT_TC_FILTER
        args.skip_mail = True
        args.disable_file_cache = True
        return args

    @property
    def output_dir(self):
        return ProjectUtils.get_test_output_child_dir(JENKINS_TEST_REPORTER)

    def test_successful_api_response_verify_failed_testcases(self):
        spec = JenkinsReportJsonSpec(
            failed={"org.somepackage3": 10, "org.somepackage4": 20},
            skipped={"org.somepackage1": 10, "org.somepackage2": 20},
            passed={"org.somepackage1": 10, "org.somepackage2": 20},
        )
        report: JenkinsTestReport = JenkinsReportGenerator.generate(spec)
        report_as_dict = dataclasses.asdict(report)
        report_json = json.dumps(report_as_dict, indent=4)

        builds: JenkinsBuilds = JenkinsBuildsGenerator.generate(BUILD_URL_MAWO_7X_TEMPLATE, latest_build_num=200)
        builds_as_dict = dataclasses.asdict(builds)
        builds_json = json.dumps(builds_as_dict, indent=4)

        httpretty.register_uri(
            httpretty.GET,
            re.compile(r"http://build.infra.cloudera.com//job/Mawo-UT-hadoop-CDPD-7.x/api/json.*"),
            body=builds_json,
        )

        httpretty.register_uri(
            httpretty.GET,
            re.compile(r"http://build.infra.cloudera.com/job/Mawo-UT-hadoop-CDPD-7.x/200/testReport/api/json.*"),
            body=report_json,
        )

        reporter = JenkinsTestReporter(self.args, self.output_dir)
        reporter.run()

        # Assert all failed testcases
        all_failed_tests_in_jenkins_report: Set[str] = set(spec.get_all_failed_testcases())
        failed_tests: Set[str] = set(reporter.failed_tests)
        self.assertEqual(30, len(failed_tests))
        self.assertEqual(30, len(all_failed_tests_in_jenkins_report))
        self.assertSetEqual(failed_tests, all_failed_tests_in_jenkins_report)

        # Assert filtered testcases
        self.assertEqual(HADOOP_TC_FILTER, reporter.testcase_filter)
        self.assertEqual(1, reporter.num_build_data)
        self.assertEqual(0, len(reporter.get_filtered_testcases_from_build(0)))
