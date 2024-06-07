import dataclasses
import json
import logging
import os
import random
import re
import tempfile
import unittest
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Dict, Tuple, Set
from unittest.mock import patch, Mock

import httpretty as httpretty
import mongomock
import pytest
from coolname import generate_slug
from pythoncommons.date_utils import DateUtils
from pythoncommons.project_utils import ProjectUtils
from pythoncommons.url_utils import UrlUtils

from tests.test_utilities import Object, TestUtilities
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.commands.unittestresultfetcher.unit_test_result_fetcher import (
    UnitTestResultFetcher,
    Email,
    CacheConfig,
    EmailConfig,
)
from yarndevtools.commands.unittestresultfetcher.jenkins import JenkinsJobUrls, JenkinsApi, DownloadProgress
from yarndevtools.common.common_model import FailedJenkinsBuild, JobBuildDataStatus, JobBuildDataCounters
from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME

EMAIL_CLASS_NAME = Email.__name__
SEND_MAIL_PATCH_PATH = "yarndevtools.commands.unittestresultfetcher.unit_test_result_fetcher.{}.send_mail".format(
    EMAIL_CLASS_NAME
)
NETWORK_UTILS_PATCH_PATH = "pythoncommons.network_utils.NetworkUtils.fetch_json"

DEFAULT_LATEST_BUILD_NUM = 215
DEFAULT_NUM_BUILDS = 51

PACK_1 = "org.somepackage1"
PACK_2 = "org.somepackage2"
PACK_3 = "org.somepackage3"
PACK_4 = "org.somepackage4"
STDOUT = "stdout"
STDERR = "stderrr"


YARN_TC_FILTER = "YARN:org.apache.hadoop.yarn"
MAPRED_TC_FILTER = "MAPREDUCE:org.apache.hadoop.mapreduce"
MULTI_FILTER = [YARN_TC_FILTER, MAPRED_TC_FILTER]

JENKINS_MAIN_URL = "http://build.infra.cloudera.com"
MAWO_JOB_NAME_7X = "Mawo-UT-hadoop-CDPD-7.x"
MAWO_JOB_NAME_71X = "Mawo-UT-hadoop-CDPD-7.1.x"
DEFAULT_JOB_NAME = MAWO_JOB_NAME_7X
BUILD_URL_ID_KEY = "build_id"
BUILD_URL_MAWO_7X_TEMPLATE = f"{JENKINS_MAIN_URL}/job/{MAWO_JOB_NAME_7X}/{{{BUILD_URL_ID_KEY}}}/"
BUILD_URL_MAWO_71X_TEMPLATE = f"{JENKINS_MAIN_URL}/job/{MAWO_JOB_NAME_71X}/{{{BUILD_URL_ID_KEY}}}/"

USE_REAL_API = False
JOB_NAME = MAWO_JOB_NAME_7X

LOG = logging.getLogger(__name__)


class TestCaseStatus(Enum):
    PASSED = 0
    FAILED = 1
    SKIPPED = 2
    REGRESSION = 3


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

    @staticmethod
    def get_arbitrary():
        spec = JenkinsReportJsonSpec.get_arbitrary()
        report_json = TestUnitTestResultFetcher._get_jenkins_report_as_json(spec)
        report_dict = json.loads(report_json)
        return report_dict, spec

    @staticmethod
    def get_with_regression():
        spec = JenkinsReportJsonSpec.get_with_regression()
        report_json = TestUnitTestResultFetcher._get_jenkins_report_as_json(spec)
        report_dict = json.loads(report_json)
        return report_dict, spec

    @staticmethod
    def get_all_green():
        spec = JenkinsReportJsonSpec.get_with_only_passed()
        report_json = TestUnitTestResultFetcher._get_jenkins_report_as_json(spec)
        report_dict = json.loads(report_json)
        return report_dict, spec

    @staticmethod
    def get_empty():
        spec = JenkinsReportJsonSpec.get_empty()
        report_json = TestUnitTestResultFetcher._get_jenkins_report_as_json(spec)
        report_dict = json.loads(report_json)
        return report_dict, spec


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
    # Key: package, value: number of testcases with result type
    failed: Dict[str, int]
    passed: Dict[str, int]
    skipped: Dict[str, int]
    regression: Dict[str, int] = dataclasses.field(default_factory=dict)
    allow_empty: bool = False

    def __post_init__(self):
        self._counts_per_status: Dict[TestCaseStatus, Dict[str, int]] = {
            TestCaseStatus.FAILED: self.failed,
            TestCaseStatus.PASSED: self.passed,
            TestCaseStatus.SKIPPED: self.skipped,
            TestCaseStatus.REGRESSION: self.regression,
        }
        self._counts = {tcs: sum(self._counts_per_status[tcs].values()) for tcs in TestCaseStatus}

        self._num_all_testcases: int = sum(self._counts.values())

        if not self.allow_empty and self._num_all_testcases < 5:
            raise ValueError("Minimum required value of all testcases is 5!")

        self._test_classnames: Dict[TestCaseStatus, List[str]] = {}
        for tcs in TestCaseStatus:
            self._test_classnames[tcs] = [self._generate_classname() for _ in range(5)]

        self._add_to_result_dict()

        actual_all_testcases = sum([len(i) for i in self._testcases_by_status.values()])
        if actual_all_testcases != self._num_all_testcases:
            raise ValueError(
                "Size of dict should be equal to number of all testcases!\n"
                f"Size of dict: {len(self._testcases_by_status)}\n"
                f"All testcases: {self._num_all_testcases}"
            )

    @property
    def failed_count(self):
        return self._counts[TestCaseStatus.FAILED]

    @property
    def passed_count(self):
        return self._counts[TestCaseStatus.PASSED]

    @property
    def skipped_count(self):
        return self._counts[TestCaseStatus.SKIPPED]

    def get_failed_testcases(self, package: str):
        testcases = self._get_by_statuses(TestCaseStatus.FAILED)
        filtered_testcases = list(filter(lambda tc: tc.startswith(package), testcases))
        return filtered_testcases

    def get_all_failed_testcases(self):
        testcases = self._get_by_statuses(TestCaseStatus.FAILED, TestCaseStatus.REGRESSION)
        return testcases

    def _get_by_statuses(self, *statuses):
        if len(statuses) == 1:
            return self._testcases_by_status[statuses[0]]
        else:
            res = []
            for status in statuses:
                if status not in self._testcases_by_status:
                    LOG.warning("Testcases for status '%s' are not stored for JenkinsReportJsonSpec", status)
                res += self._testcases_by_status[status]
            return res

    @property
    def len_testcases(self):
        return len(self._testcases_by_status)

    @staticmethod
    def _generate_classname(words=3):
        gen = generate_slug(words)
        comps = gen.split("-")
        return "".join([f"{c[0].upper()}{c[1:]}" for c in comps])

    def _add_to_result_dict(self):
        self._testcases_by_status: Dict[TestCaseStatus, List[str]] = defaultdict(
            list
        )  # Key: testcase FQN, value: status
        self.testcases_by_suites: Dict[str, List[Tuple[str, TestCaseStatus]]] = defaultdict(
            list
        )  # Key: class name FQN, value:

        for status in TestCaseStatus:
            package_to_count = self._counts_per_status[status]
            classnames = self._test_classnames[status]
            for package, num_testcases in package_to_count.items():
                for idx in range(0, num_testcases):
                    class_name_fqn, tc_fqn, tc_name = self._generate_testcase_fqn(status, classnames, idx, package)
                    self._testcases_by_status[status].append(tc_fqn)
                    self.testcases_by_suites[class_name_fqn].append((tc_name, status))

    @staticmethod
    def _generate_testcase_fqn(status, classnames, idx, package):
        tc_name = f"{status.name.lower()}-tc-{generate_slug(2)}"
        class_name = classnames[idx % 5]
        class_name_fqn = f"{package}.{class_name}"
        tc_fqn = f"{class_name_fqn}.{tc_name}"
        return class_name_fqn, tc_fqn, tc_name

    @staticmethod
    def get_arbitrary():
        spec = JenkinsReportJsonSpec(
            failed={
                PACK_3: 10,
                PACK_4: 20,
                TestUnitTestResultFetcher._get_package_from_filter(YARN_TC_FILTER): 5,
                TestUnitTestResultFetcher._get_package_from_filter(MAPRED_TC_FILTER): 10,
            },
            skipped={PACK_1: 10, PACK_2: 20},
            passed={PACK_1: 10, PACK_2: 20},
        )
        return spec

    @staticmethod
    def get_with_regression():
        spec = JenkinsReportJsonSpec(
            failed={
                PACK_3: 10,
                PACK_4: 20,
                TestUnitTestResultFetcher._get_package_from_filter(YARN_TC_FILTER): 5,
                TestUnitTestResultFetcher._get_package_from_filter(MAPRED_TC_FILTER): 10,
            },
            passed={},
            skipped={},
            regression={PACK_1: 15, PACK_2: 25},
        )
        return spec

    @staticmethod
    def get_with_only_passed():
        spec = JenkinsReportJsonSpec(
            failed={},
            skipped={},
            passed={PACK_1: 10, PACK_2: 20},
        )
        return spec

    @staticmethod
    def get_empty():
        spec = JenkinsReportJsonSpec(failed={}, skipped={}, passed={}, allow_empty=True)
        return spec


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
    def generate(
        build_url_template: str, num_builds: int = DEFAULT_NUM_BUILDS, latest_build_num: int = DEFAULT_LATEST_BUILD_NUM
    ) -> JenkinsBuilds:
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


class TestUnitTestResultFetcher(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Invoke this to setup main output directory and avoid test failures while initing config
        cls.project_out_root = ProjectUtils.get_test_output_basedir(YARNDEVTOOLS_MODULE_NAME)
        ProjectUtils.get_test_output_child_dir(CommandType.UNIT_TEST_RESULT_FETCHER.output_dir_name)

    @classmethod
    def tearDownClass(cls) -> None:
        TestUtilities.tearDownClass(cls.__name__, command_type=CommandType.UNIT_TEST_RESULT_FETCHER)

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
        job_names: str = JOB_NAME,
        jenkins_url: str = JENKINS_MAIN_URL,
        num_builds: str = "14",  # input should be string as JENKINS_BUILDS_EXAMINE_UNLIMITIED_VAL is a special value
        skip_sending_mail: bool = False,
        force_sending_mail: bool = True,
        force_mode: bool = True,
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
        args.subject = "Test email subject"
        args.force_send_email = force_sending_mail
        args.jenkins_url = jenkins_url
        args.job_names = job_names
        args.num_builds = num_builds
        args.tc_filters = tc_filters
        args.skip_mail = skip_sending_mail
        args.disable_file_cache = True
        args.logging_debug = True
        args.verbose = True
        args.command = CommandType.UNIT_TEST_RESULT_FETCHER.real_name
        args.force_mode = force_mode

        args.mongo_hostname = "mongo.example.com"
        args.mongo_port = 27017
        args.mongo_user = "mongo_user"
        args.mongo_password = "mongo_password"
        args.mongo_db_name = "mongo_db_name"
        args.mongo_force_create_db = True
        return args

    @property
    def output_dir(self):
        return ProjectUtils.get_test_output_child_dir(CommandType.UNIT_TEST_RESULT_FETCHER.output_dir_name)

    @staticmethod
    def _get_jenkins_report_as_json(spec: JenkinsReportJsonSpec):
        report: JenkinsTestReport = JenkinsReportGenerator.generate(spec)
        report_as_dict = dataclasses.asdict(report)
        report_json = json.dumps(report_as_dict, indent=4)
        return report_json

    @staticmethod
    def _get_default_jenkins_builds_as_json(build_id=200):
        builds_as_dict = TestUnitTestResultFetcher._get_default_jenkins_builds_as_dict(build_id)
        builds_json = json.dumps(builds_as_dict, indent=4)
        return build_id, builds_json

    @staticmethod
    def _get_default_jenkins_builds_as_dict(build_id):
        builds: JenkinsBuilds = JenkinsBuildsGenerator.generate(BUILD_URL_MAWO_7X_TEMPLATE, latest_build_num=build_id)
        builds_as_dict = dataclasses.asdict(builds)
        return builds_as_dict

    @staticmethod
    def _mock_jenkins_report_api(report_json, jenkins_url=JENKINS_MAIN_URL, job_name=DEFAULT_JOB_NAME, build_id=200):
        build_url = TestUnitTestResultFetcher.get_build_url(jenkins_url, job_name, build_id)
        final_url = rf"{build_url}/testReport/api/json.*"
        final_url = UrlUtils.sanitize_url(final_url)
        LOG.info("Mocked URL: %s", final_url)
        httpretty.register_uri(
            httpretty.GET,
            re.compile(final_url),
            body=report_json,
        )

    @staticmethod
    def _mock_jenkins_build_api(
        builds_json,
        jenkins_url=JENKINS_MAIN_URL,
        job_name=JOB_NAME,
    ):
        job_url = TestUnitTestResultFetcher.get_job_url(jenkins_url, job_name)
        final_url = rf"{job_url}/api/json.*"
        final_url = UrlUtils.sanitize_url(final_url)
        LOG.info("Mocked URL: %s", final_url)
        httpretty.register_uri(
            httpretty.GET,
            re.compile(final_url),
            body=builds_json,
        )

    @staticmethod
    def get_job_url(jenkins_url: str, job_name: str):
        if jenkins_url.endswith("/"):
            jenkins_url = jenkins_url[:-1]
        return UrlUtils.sanitize_url(f"{jenkins_url}/job/{job_name}/")

    @staticmethod
    def get_build_url(jenkins_url: str, job_name: str, build_id: int):
        if jenkins_url.endswith("/"):
            jenkins_url = jenkins_url[:-1]
        job_url = TestUnitTestResultFetcher.get_job_url(jenkins_url, job_name)
        return UrlUtils.sanitize_url(f"{job_url}/{build_id}/")

    def _assert_all_failed_testcases(
        self, reporter: UnitTestResultFetcher, spec, expected_failed_count=-1, job_name=DEFAULT_JOB_NAME
    ):
        all_failed_tests_in_jenkins_report: Set[str] = set(spec.get_all_failed_testcases())
        failed_tests: Set[str] = set(reporter.get_failed_tests(job_name))
        self.assertEqual(expected_failed_count, len(failed_tests))
        self.assertEqual(expected_failed_count, len(all_failed_tests_in_jenkins_report))
        self.assertSetEqual(failed_tests, all_failed_tests_in_jenkins_report)

    def _assert_num_filtered_testcases_single_build(
        self,
        reporter: UnitTestResultFetcher,
        filters: List[str] = None,
        expected_num_build_data=-1,
        expected_failed_testcases_dict: Dict[str, List[str]] = None,
        job_name=DEFAULT_JOB_NAME,
        job_url: str = None,
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
        self.assertEqual(expected_num_build_data, reporter.get_num_build_data(job_name))

        for tc_filter in filters:
            package = self._get_package_from_filter(tc_filter)
            actual_failed_testcases = reporter.get_filtered_testcases_from_build(job_url, package, job_name)
            expected_failed_testcases: List[str] = expected_failed_testcases_dict[tc_filter]
            self.assertEqual(len(expected_failed_testcases), len(actual_failed_testcases))
            self.assertListEqual(sorted(actual_failed_testcases), sorted(expected_failed_testcases))

    def _assert_send_mail(self, mock_send_mail_call):
        self.assertEqual(mock_send_mail_call.call_count, 1)
        report_result = mock_send_mail_call.call_args_list[0]
        LOG.info("Report result: %s", report_result)
        self.assertTrue(
            report_result.startswith(
                "Counters:\nFailed: 30, Passed: 30, Build number: 200\n, "
                "Build URL: http://build.infra.cloudera.com/job/Mawo-UT-hadoop-CDPDP-7.x/200/"
            )
        )

    @patch(SEND_MAIL_PATCH_PATH)
    @mongomock.patch(servers=(("mongo.example.com", 27017),))
    def test_successful_api_response_verify_failed_testcases(self, mock_send_mail_call):
        # TODO Re-enable test once mongomock PR is merged / created
        spec = JenkinsReportJsonSpec(
            failed={PACK_3: 10, PACK_4: 20},
            skipped={PACK_1: 10, PACK_2: 20},
            passed={PACK_1: 10, PACK_2: 20},
        )
        build_id, builds_json = self._get_default_jenkins_builds_as_json(build_id=200)
        report_json = self._get_jenkins_report_as_json(spec)
        self._mock_jenkins_build_api(builds_json)
        self._mock_jenkins_report_api(report_json, build_id=200)

        reporter = UnitTestResultFetcher(self.generate_args(), self.output_dir)
        reporter.run()
        job_url = TestUnitTestResultFetcher.get_build_url(JENKINS_MAIN_URL, DEFAULT_JOB_NAME, 200)
        self._assert_send_mail(mock_send_mail_call)
        self._assert_all_failed_testcases(reporter, spec, expected_failed_count=30)
        self._assert_num_filtered_testcases_single_build(
            reporter,
            filters=[YARN_TC_FILTER],
            expected_num_build_data=1,
            expected_failed_testcases_dict={YARN_TC_FILTER: []},
            job_url=job_url,
        )

    @patch(SEND_MAIL_PATCH_PATH)
    @mongomock.patch(servers=(("mongo.example.com", 27017),))
    def test_successful_api_response_verify_filtered_testcases(self, mock_send_mail_call):
        # TODO Re-enable test once mongomock PR is merged / created
        spec = JenkinsReportJsonSpec(
            failed={PACK_3: 10, PACK_4: 20, self._get_package_from_filter(YARN_TC_FILTER): 25},
            skipped={PACK_1: 10, PACK_2: 20},
            passed={PACK_1: 10, PACK_2: 20},
        )
        failed_testcases: List[str] = spec.get_failed_testcases(self._get_package_from_filter(YARN_TC_FILTER))
        build_id, builds_json = self._get_default_jenkins_builds_as_json(build_id=200)
        report_json = self._get_jenkins_report_as_json(spec)
        self._mock_jenkins_build_api(builds_json)
        self._mock_jenkins_report_api(report_json, build_id=200)

        reporter = UnitTestResultFetcher(self.generate_args(), self.output_dir)
        reporter.run()
        job_url = TestUnitTestResultFetcher.get_build_url(JENKINS_MAIN_URL, DEFAULT_JOB_NAME, 200)
        self._assert_send_mail(mock_send_mail_call)
        self._assert_all_failed_testcases(reporter, spec, expected_failed_count=55)
        self._assert_num_filtered_testcases_single_build(
            reporter,
            filters=[YARN_TC_FILTER],
            expected_num_build_data=1,
            expected_failed_testcases_dict={YARN_TC_FILTER: failed_testcases},
            job_url=job_url,
        )

    @patch(SEND_MAIL_PATCH_PATH)
    @mongomock.patch(servers=(("mongo.example.com", 27017),))
    def test_successful_api_response_verify_multi_filtered(self, mock_send_mail_call):
        # TODO Re-enable test once mongomock PR is merged / created
        spec = JenkinsReportJsonSpec.get_arbitrary()
        failed_yarn_testcases: List[str] = spec.get_failed_testcases(self._get_package_from_filter(YARN_TC_FILTER))
        failed_mr_testcases: List[str] = spec.get_failed_testcases(self._get_package_from_filter(MAPRED_TC_FILTER))
        build_id, builds_json = self._get_default_jenkins_builds_as_json(build_id=200)
        report_json = self._get_jenkins_report_as_json(spec)
        self._mock_jenkins_build_api(builds_json)
        self._mock_jenkins_report_api(report_json, build_id=200)

        reporter = UnitTestResultFetcher(self.generate_args(tc_filters=MULTI_FILTER), self.output_dir)
        reporter.run()
        job_url = TestUnitTestResultFetcher.get_build_url(JENKINS_MAIN_URL, DEFAULT_JOB_NAME, 200)
        self._assert_send_mail(mock_send_mail_call)
        self._assert_all_failed_testcases(reporter, spec, expected_failed_count=45)
        self._assert_num_filtered_testcases_single_build(
            reporter,
            filters=MULTI_FILTER,
            expected_num_build_data=1,
            expected_failed_testcases_dict={
                YARN_TC_FILTER: failed_yarn_testcases,
                MAPRED_TC_FILTER: failed_mr_testcases,
            },
            job_url=job_url,
        )

    # TODO Add TC to test cache loading

    @staticmethod
    def _get_package_from_filter(filter: str):
        return filter.split(":")[-1]

    def test_jenkins_api_parse_job_data_check_failed_testcases(self):
        report_dict, spec = JenkinsTestReport.get_arbitrary()
        failed_jenkins_build = FailedJenkinsBuild(TestUnitTestResultFetcher.get_arbitrary_build_url(), 12345, "testJob")
        job_build_data = JenkinsApi.parse_job_data(report_dict, failed_jenkins_build)
        self.assertEqual(set(spec.get_all_failed_testcases()), job_build_data.failed_testcases)
        self.assertEqual(JobBuildDataStatus.HAVE_FAILED_TESTCASES, job_build_data.status)

    def test_jenkins_api_parse_job_data_check_regression_testcases(self):
        report_dict, spec = JenkinsTestReport.get_with_regression()
        failed_jenkins_build = FailedJenkinsBuild(TestUnitTestResultFetcher.get_arbitrary_build_url(), 12345, "testJob")
        job_build_data = JenkinsApi.parse_job_data(report_dict, failed_jenkins_build)
        self.assertEqual(set(spec.get_all_failed_testcases()), job_build_data.failed_testcases)
        self.assertEqual(JobBuildDataStatus.HAVE_FAILED_TESTCASES, job_build_data.status)

    def test_jenkins_api_parse_job_data_check_counters(self):
        report_dict, spec = JenkinsTestReport.get_arbitrary()
        failed_jenkins_build = FailedJenkinsBuild(TestUnitTestResultFetcher.get_arbitrary_build_url(), 12345, "testJob")
        job_build_data = JenkinsApi.parse_job_data(report_dict, failed_jenkins_build)

        exp_counter = JobBuildDataCounters(failed=45, passed=30, skipped=30)
        self.assertEqual(exp_counter, job_build_data.counters)
        self.assertEqual(JobBuildDataStatus.HAVE_FAILED_TESTCASES, job_build_data.status)

    def test_jenkins_api_parse_job_data_check_status_counters_all_green_job(self):
        report_dict, spec = JenkinsTestReport.get_all_green()
        failed_jenkins_build = FailedJenkinsBuild(TestUnitTestResultFetcher.get_arbitrary_build_url(), 12345, "testJob")
        job_build_data = JenkinsApi.parse_job_data(report_dict, failed_jenkins_build)
        self.assertIsNone(job_build_data.counters)
        self.assertEqual(JobBuildDataStatus.ALL_GREEN, job_build_data.status)

    def test_jenkins_api_parse_job_data_check_status_counters_empty_job(self):
        report_dict, spec = JenkinsTestReport.get_empty()
        failed_jenkins_build = FailedJenkinsBuild(TestUnitTestResultFetcher.get_arbitrary_build_url(), 12345, "testJob")
        job_build_data = JenkinsApi.parse_job_data(report_dict, failed_jenkins_build)
        self.assertIsNone(job_build_data.counters)
        self.assertEqual(JobBuildDataStatus.EMPTY, job_build_data.status)

    @patch(NETWORK_UTILS_PATCH_PATH)
    def test_jenkins_api_convert_latest_job(self, mock_fetch_json):
        jenkins_api = JenkinsApi(None, None)
        builds_dict = self._get_default_jenkins_builds_as_dict(build_id=200)
        sorted_builds_desc = sorted(builds_dict["builds"], key=lambda x: x["url"], reverse=True)
        mock_fetch_json.return_value = builds_dict
        job_name = "test_job"
        jenkins_urls: JenkinsJobUrls = JenkinsJobUrls(JENKINS_MAIN_URL, job_name)
        failed_builds, total_no_of_builds = jenkins_api.list_builds_for_job(job_name, jenkins_urls, days=1)
        # fetch_builds_url = mock_fetch_json.call_args_list[0]
        # self.assertEqual("'http://jenkins_base_url/job/test_job/api/json?tree=builds[url,result,timestamp]'", fetch_builds_url)
        self.assertEqual(1, len(failed_builds))

        exp_latest_build = sorted_builds_desc[0]
        act_latest_build = failed_builds[0]
        self.assertEqual(job_name, act_latest_build.job_name)
        self.assertEqual(int(int(exp_latest_build["timestamp"]) / 1000), int(act_latest_build.timestamp))
        self.assertEqual(exp_latest_build["url"], act_latest_build.url)
        self.assertEqual(DEFAULT_NUM_BUILDS, total_no_of_builds)

    @patch(NETWORK_UTILS_PATCH_PATH)
    def test_jenkins_api_convert_more_jobs(self, mock_fetch_json):
        jenkins_api = JenkinsApi(None, None)
        builds_dict = self._get_default_jenkins_builds_as_dict(build_id=200)
        sorted_builds_desc = sorted(builds_dict["builds"], key=lambda x: x["url"], reverse=True)
        mock_fetch_json.return_value = builds_dict
        job_name = "test_job"
        jenkins_urls: JenkinsJobUrls = JenkinsJobUrls(JENKINS_MAIN_URL, job_name)
        failed_builds, total_no_of_builds = jenkins_api.list_builds_for_job(job_name, jenkins_urls, days=16)
        # fetch_builds_url = mock_fetch_json.call_args_list[0]
        # self.assertEqual("'http://jenkins_base_url/job/test_job/api/json?tree=builds[url,result,timestamp]'", fetch_builds_url)
        self.assertEqual(16, len(failed_builds))
        self.assertEqual(DEFAULT_NUM_BUILDS, total_no_of_builds)

        for i in range(16):
            exp_build = sorted_builds_desc[i]
            act_build = failed_builds[i]
            self.assertEqual(job_name, act_build.job_name)
            self.assertEqual(int(int(exp_build["timestamp"]) / 1000), int(act_build.timestamp))
            self.assertEqual(exp_build["url"], act_build.url)

    @patch(NETWORK_UTILS_PATCH_PATH)
    def test_jenkins_api_download_job_result(self, mock_fetch_json: Mock):
        jenkins_api = JenkinsApi(None, None)
        builds_dict = self._get_default_jenkins_builds_as_dict(build_id=200)
        failed_build = FailedJenkinsBuild("http://full/url/of/job/42", 1244525, "test_job")
        mock_fetch_json.return_value = builds_dict

        act_test_report = jenkins_api.download_job_result(failed_build, Mock(spec=DownloadProgress))

        LOG.debug("Call args list: %s", mock_fetch_json.call_args_list)
        self.assertEqual(builds_dict, act_test_report)
        self.assertEqual(
            "http://full/url/of/job/42/testReport/api/json?pretty=true", mock_fetch_json.call_args_list[0].args[0]
        )

    def test_cache_config_without_any_setting(self):
        args = Object()
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_config = CacheConfig(args, tmp_dir)
            self.assertTrue(cache_config.enabled)
            self.assertTrue(os.path.isdir(cache_config.reports_dir))
            self.assertEqual(cache_config.reports_dir, os.path.join(tmp_dir, "reports"))

            self.assertTrue(os.path.isdir(cache_config.cached_data_dir))
            self.assertEqual(cache_config.cached_data_dir, os.path.join(tmp_dir, "cached_data"))

            self.assertFalse(cache_config.download_uncached_job_data)

    def test_cache_config_with_settings(self):
        args = Object()
        args.disable_file_cache = False
        args.download_uncached_job_data = True
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_config = CacheConfig(args, tmp_dir)
            self.assertTrue(cache_config.enabled)
            self.assertTrue(os.path.isdir(cache_config.reports_dir))
            self.assertEqual(cache_config.reports_dir, os.path.join(tmp_dir, "reports"))

            self.assertTrue(os.path.isdir(cache_config.cached_data_dir))
            self.assertEqual(cache_config.cached_data_dir, os.path.join(tmp_dir, "cached_data"))

            self.assertTrue(cache_config.download_uncached_job_data)

    def test_email_config_with_default_settings(self):
        args = self._create_args_for_full_email_config()
        config = EmailConfig(args)
        self.assertFalse(config.force_send_email)
        self.assertTrue(config.send_mail)
        self.assertEqual([], config.reset_email_send_state)

    def test_email_config_with_skip_email_and_without_force_sending_email(self):
        args = self._create_args_for_full_email_config()
        args.skip_email = True
        args.force_send_email = False
        config = EmailConfig(args)
        self.assertFalse(config.force_send_email)
        self.assertFalse(config.send_mail)
        self.assertEqual([], config.reset_email_send_state)

    def test_email_config_with_skip_email_and_with_force_sending_email(self):
        args = self._create_args_for_full_email_config()
        args.skip_email = True
        args.force_send_email = True
        config = EmailConfig(args)
        self.assertTrue(config.force_send_email)
        self.assertTrue(config.send_mail)
        self.assertEqual([], config.reset_email_send_state)

    def test_email_config_validate_job_names_to_reset_state_for_unknown_job(self):
        args = self._create_args_for_full_email_config()
        args.skip_email = True
        args.force_send_email = True
        args.reset_send_state_for_jobs = "job3"
        config = EmailConfig(args)
        with self.assertRaises(ValueError):
            config.validate(["job1", "job2"])

    def test_email_config_validate_job_names_to_reset_state_for_known_job(self):
        args = self._create_args_for_full_email_config()
        args.skip_email = True
        args.force_send_email = True
        args.reset_send_state_for_jobs = ["job1", "job2"]
        config = EmailConfig(args)
        config.validate(["job1", "job2"])

    def test_email_config_validate_job_names_to_reset_state_for_some_unknown_job(self):
        args = self._create_args_for_full_email_config()
        args.skip_email = True
        args.force_send_email = True
        args.reset_send_state_for_jobs = ["job1", "job2", "job999"]
        config = EmailConfig(args)
        with self.assertRaises(ValueError):
            config.validate(["job1", "job2"])

    def _create_args_for_full_email_config(self):
        args = Object()
        args.account_user = "someUser"
        args.account_password = "somePassword"
        args.smtp_server = "smtpServer"
        args.smtp_port = "smtpPort"
        args.sender = "sender"
        args.subject = "subject"
        args.recipients = ["recipient1", "recipient2"]
        args.attachment_filename = "attachmentFilename"
        return args

    @staticmethod
    def get_arbitrary_build_url():
        return BUILD_URL_MAWO_7X_TEMPLATE.format(**{BUILD_URL_ID_KEY: 200})
