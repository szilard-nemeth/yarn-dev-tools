import os
import re
import tempfile
import unittest
from typing import Dict

from httpretty import httpretty
from pythoncommons.file_utils import FileUtils, FindResultType
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils, ProjectRootDeterminationStrategy
from pythoncommons.string_utils import StringUtils

from tests.cdsw.common.test_cdsw_runner import FakeCdswRunner
from tests.cdsw.common.testutils.cdsw_testing_common import (
    CdswTestingCommons,
    CommandExpectations,
    COMMAND_ARGUMENTS_COMMON,
)
from tests.test_utilities import Object
from yarndevtools.cdsw.cdsw_common import CommonFiles, CdswSetup, GenericCdswConfigUtils, BASHX
from yarndevtools.cdsw.cdsw_runner import CdswRunnerConfig, CdswConfigReaderAdapter
from yarndevtools.cdsw.constants import CdswEnvVar
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME, UPSTREAM_JIRA_BASE_URL, PYTHON3
import logging

USE_LIVE_JIRA_SERVER = False
PARSER = None
SETUP_RESULT = None
CDSW_RUNNER_SCRIPT_PATH = None
LOG = logging.getLogger(__name__)
JIRA_UMBRELLA_FETCHER_UPSTREAM_UMBRELLA_IDS = ["YARN-10496", "YARN-6223"]


# TODO Extract code as much as possible
class TestCdswRunnerJobsE2E(unittest.TestCase):
    yarn_dev_tools_script_path = None

    ENV_VARS = [
        "GSHEET_CLIENT_SECRET",
        "GSHEET_WORKSHEET",
        "GSHEET_SPREADSHEET",
        "GSHEET_JIRA_COLUMN",
        "GSHEET_UPDATE_DATE_COLUMN",
        "GSHEET_STATUS_INFO_COLUMN",
        "BRANCHES",
        "MAIL_ACC_USER",
        "MAIL_ACC_PASSWORD",
    ]

    @classmethod
    def setUpClass(cls) -> None:
        OsUtils.clear_env_vars([CdswEnvVar.MAIL_RECIPIENTS.name])

        # TODO Investigate this later to check why number of loggers are not correct
        OsUtils.set_env_value("ENABLE_LOGGER_HANDLER_SANITY_CHECK", "False")
        ProjectUtils.set_root_determine_strategy(ProjectRootDeterminationStrategy.COMMON_FILE)
        ProjectUtils.get_test_output_basedir(YARNDEVTOOLS_MODULE_NAME)

        # We need the value of 'CommonFiles.YARN_DEV_TOOLS_SCRIPT'
        CdswSetup._setup_python_module_root_and_yarndevtools_path()
        cls.yarn_dev_tools_script_path = CommonFiles.YARN_DEV_TOOLS_SCRIPT

        COMMAND_ARGUMENTS_COMMON[cls.yarn_dev_tools_script_path] = 0

    def setUp(self) -> None:
        self.cdsw_testing_commons = CdswTestingCommons()
        CdswTestingCommons.mock_google_drive()

        self.exp_command_clone_downstream_repos = CommandExpectations(self).with_exact_command_expectation(
            f"{BASHX} /home/cdsw/scripts/clone_downstream_repos.sh"
        )

        self.exp_command_clone_upstream_repos = CommandExpectations(self).with_exact_command_expectation(
            f"{BASHX} /home/cdsw/scripts/clone_upstream_repos.sh"
        )
        if not USE_LIVE_JIRA_SERVER:
            httpretty.enable()
            self._setup_mock_responses_for_upstream_jira()

    def tearDown(self) -> None:
        self._clear_env_vars()
        CdswTestingCommons.mock_google_drive()

        if not USE_LIVE_JIRA_SERVER:
            # disable afterwards, so that you will have no problems in code that uses that socket module
            httpretty.disable()
            # reset HTTPretty state (clean up registered urls and request history)
            httpretty.reset()

    def _setup_mock_responses_for_upstream_jira(self):
        for umbrella_id in JIRA_UMBRELLA_FETCHER_UPSTREAM_UMBRELLA_IDS:
            html_file_path = FileUtils.find_files(
                self.cdsw_testing_commons.cdsw_tests_root_dir,
                find_type=FindResultType.FILES,
                regex=f"jira_{umbrella_id}.html",
                single_level=False,
                full_path_result=True,
                ensure_number_of_results=1,
            )[0]
            url = UPSTREAM_JIRA_BASE_URL + umbrella_id
            LOG.info("Mocked URL: %s with file contents of file: %s", url, html_file_path)
            httpretty.register_uri(httpretty.GET, re.compile(url), body=FileUtils.read_file(html_file_path))

    @classmethod
    def _clear_env_vars(cls):
        for var in cls.ENV_VARS:
            if var in os.environ:
                del os.environ[var]

    @staticmethod
    def _create_args_for_specified_file(config_file: str, cmd_type: CommandType, dry_run: bool = True):
        args = Object()
        args.config_file = config_file
        args.debug = True
        args.verbose = True
        args.cmd_type = cmd_type.name
        args.dry_run = dry_run
        return args

    @staticmethod
    def _set_env_vars_from_dict(dict_of_vars: Dict[str, str]):
        for k, v in dict_of_vars.items():
            os.environ[k] = str(v)

    def test_reviewsync_e2e(self):
        config_file = FileUtils.find_files(
            self.cdsw_testing_commons.cdsw_root_dir,
            find_type=FindResultType.FILES,
            regex="reviewsync_.*",
            single_level=False,
            full_path_result=True,
            exclude_dirs=["yarndevtools-results"],
        )[0]

        self._set_env_vars_from_dict(
            {
                "GSHEET_CLIENT_SECRET": "testGsheetClientSecret",
                "GSHEET_WORKSHEET": "testGsheetWorkSheet",
                "GSHEET_SPREADSHEET": "testGsheetSpreadSheet",
                "GSHEET_JIRA_COLUMN": "testGsheetJiraColumn",
                "GSHEET_UPDATE_DATE_COLUMN": "testGsheetUpdateDateColumn",
                "GSHEET_STATUS_INFO_COLUMN": "testGsheetStatusInfoColumn",
                "BRANCHES": "branch-3.2 branch-3.3",
                "MAIL_ACC_USER": "testMailUser",
                "MAIL_ACC_PASSWORD": "testMailPassword",
            }
        )

        args = self._create_args_for_specified_file(config_file, CommandType.REVIEWSYNC, dry_run=True)
        cdsw_runner_config = CdswRunnerConfig(PARSER, args, config_reader=CdswConfigReaderAdapter())
        cdsw_runner = FakeCdswRunner(cdsw_runner_config)
        cdsw_runner.start()

        exp_command_1 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg(self.yarn_dev_tools_script_path)
            .add_expected_ordered_arg("REVIEWSYNC")
            .add_expected_arg("--gsheet")
            .add_expected_arg("--debug")
            .add_expected_arg("--gsheet-client-secret", "testGsheetClientSecret")
            .add_expected_arg("--gsheet-worksheet", "testGsheetWorkSheet")
            .add_expected_arg("--gsheet-spreadsheet", "testGsheetSpreadSheet")
            .add_expected_arg("--gsheet-jira-column", "testGsheetJiraColumn")
            .add_expected_arg("--gsheet-update-date-column", "testGsheetUpdateDateColumn")
            .add_expected_arg("--gsheet-status-info-column", "testGsheetStatusInfoColumn")
            .add_expected_arg("--branches", "branch-3.2 branch-3.3")
            .with_command_type(CommandType.REVIEWSYNC)
        )

        exp_command_2 = self._get_expected_zip_latest_command_data_command(CommandType.REVIEWSYNC)

        job_start_date = cdsw_runner.job_config.job_start_date()

        wrap_d = StringUtils.wrap_to_quotes
        wrap_s = StringUtils.wrap_to_single_quotes
        expected_html_link = wrap_s(f'<a href="dummy_link">Command data file: command_data_{job_start_date}.zip</a>')
        exp_command_3 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg(self.yarn_dev_tools_script_path)
            .add_expected_ordered_arg("SEND_LATEST_COMMAND_DATA")
            .add_expected_arg("--debug")
            .add_expected_arg("--smtp_server", wrap_d("smtp.gmail.com"))
            .add_expected_arg("--smtp_port", "465")
            .add_expected_arg("--account_user", wrap_d("testMailUser"))
            .add_expected_arg("--account_password", wrap_d("testMailPassword"))
            .add_expected_arg("--subject", wrap_d(f"YARN reviewsync report [start date: {job_start_date}]"))
            .add_expected_arg("--sender", wrap_d("YARN reviewsync"))
            .add_expected_arg("--recipients", wrap_d("yarn_eng_bp@cloudera.com"))
            .add_expected_arg("--attachment-filename", f"command_data_{job_start_date}.zip")
            .add_expected_arg("--file-as-email-body-from-zip", "summary.html")
            .add_expected_arg("--prepend_email_body_with_text", expected_html_link)
            .add_expected_arg("--send-attachment")
            .with_command_type(CommandType.SEND_LATEST_COMMAND_DATA)
        )

        expectations = [exp_command_1, exp_command_2, exp_command_3]
        CdswTestingCommons.verify_commands(self, expectations, cdsw_runner.executed_commands)

    def test_review_sheet_backport_updater_e2e(self):
        cdsw_root_dir: str = self.cdsw_testing_commons.cdsw_root_dir
        config_file = FileUtils.find_files(
            cdsw_root_dir,
            find_type=FindResultType.FILES,
            regex="review_sheet_backport_updater_.*",
            single_level=False,
            full_path_result=True,
            exclude_dirs=["yarndevtools-results"],
        )[0]

        self._set_env_vars_from_dict(
            {
                "GSHEET_CLIENT_SECRET": "testGsheetClientSecret",
                "GSHEET_WORKSHEET": "testGsheetWorkSheet",
                "GSHEET_SPREADSHEET": "testGsheetSpreadSheet",
                "GSHEET_JIRA_COLUMN": "testGsheetJiraColumn",
                "GSHEET_UPDATE_DATE_COLUMN": "testGsheetUpdateDateColumn",
                "GSHEET_STATUS_INFO_COLUMN": "testGsheetStatusInfoColumn",
                "BRANCHES": "branch-3.2 branch-3.3",
                "MAIL_ACC_USER": "testMailUser",
                "MAIL_ACC_PASSWORD": "testMailPassword",
            }
        )

        args = self._create_args_for_specified_file(
            config_file, CommandType.REVIEW_SHEET_BACKPORT_UPDATER, dry_run=True
        )
        cdsw_runner_config = CdswRunnerConfig(PARSER, args, config_reader=CdswConfigReaderAdapter())
        cdsw_runner = FakeCdswRunner(cdsw_runner_config)
        cdsw_runner.start()

        exp_command_1 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg(self.yarn_dev_tools_script_path)
            .add_expected_ordered_arg("REVIEW_SHEET_BACKPORT_UPDATER")
            .add_expected_arg("--debug")
            .add_expected_arg("--gsheet-client-secret", "testGsheetClientSecret")
            .add_expected_arg("--gsheet-worksheet", "testGsheetWorkSheet")
            .add_expected_arg("--gsheet-spreadsheet", "testGsheetSpreadSheet")
            .add_expected_arg("--gsheet-jira-column", "testGsheetJiraColumn")
            .add_expected_arg("--gsheet-update-date-column", "testGsheetUpdateDateColumn")
            .add_expected_arg("--gsheet-status-info-column", "testGsheetStatusInfoColumn")
            .add_expected_arg("--branches", "branch-3.2 branch-3.3")
            .with_command_type(CommandType.REVIEW_SHEET_BACKPORT_UPDATER)
        )

        exp_command_2 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg(self.yarn_dev_tools_script_path)
            .add_expected_ordered_arg("ZIP_LATEST_COMMAND_DATA")
            .add_expected_ordered_arg("REVIEW_SHEET_BACKPORT_UPDATER")
            .add_expected_arg("--debug")
            .add_expected_arg("--dest_dir", "/tmp")
            .add_expected_arg("--ignore-filetypes", "java js")
            .with_command_type(CommandType.ZIP_LATEST_COMMAND_DATA)
        )

        job_start_date = cdsw_runner.job_config.job_start_date()

        wrap_d = StringUtils.wrap_to_quotes
        wrap_s = StringUtils.wrap_to_single_quotes
        expected_html_link = wrap_s(f'<a href="dummy_link">Command data file: command_data_{job_start_date}.zip</a>')
        exp_command_3 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg(self.yarn_dev_tools_script_path)
            .add_expected_ordered_arg("SEND_LATEST_COMMAND_DATA")
            .add_expected_arg("--debug")
            .add_expected_arg("--smtp_server", wrap_d("smtp.gmail.com"))
            .add_expected_arg("--smtp_port", "465")
            .add_expected_arg("--account_user", wrap_d("testMailUser"))
            .add_expected_arg("--account_password", wrap_d("testMailPassword"))
            .add_expected_arg(
                "--subject", wrap_d(f"YARN review sheet backport updater report [start date: {job_start_date}]")
            )
            .add_expected_arg("--sender", wrap_d("YARN review sheet backport updater"))
            .add_expected_arg("--recipients", wrap_d("yarn_eng_bp@cloudera.com"))
            .add_expected_arg("--attachment-filename", f"command_data_{job_start_date}.zip")
            .add_expected_arg("--file-as-email-body-from-zip", "summary.html")
            .add_expected_arg("--prepend_email_body_with_text", expected_html_link)
            .add_expected_arg("--send-attachment")
            .with_command_type(CommandType.SEND_LATEST_COMMAND_DATA)
        )

        expectations = [exp_command_1, exp_command_2, exp_command_3]
        CdswTestingCommons.verify_commands(self, expectations, cdsw_runner.executed_commands)

    def test_unit_test_result_fetcher_e2e(self):
        cdsw_root_dir: str = self.cdsw_testing_commons.cdsw_root_dir
        config_file = FileUtils.find_files(
            cdsw_root_dir,
            find_type=FindResultType.FILES,
            regex="unit_test_result_fetcher.*",
            single_level=False,
            full_path_result=True,
            exclude_dirs=["yarndevtools-results"],
        )[0]

        self._set_env_vars_from_dict(
            {
                "MAIL_ACC_USER": "testMailUser",
                "MAIL_ACC_PASSWORD": "testMailPassword",
            }
        )
        wrap_d = StringUtils.wrap_to_quotes
        args = self._create_args_for_specified_file(config_file, CommandType.UNIT_TEST_RESULT_FETCHER, dry_run=True)
        cdsw_runner_config = CdswRunnerConfig(PARSER, args, config_reader=CdswConfigReaderAdapter())
        cdsw_runner = FakeCdswRunner(cdsw_runner_config)
        cdsw_runner.start()

        exp_command_1 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg(self.yarn_dev_tools_script_path)
            .add_expected_ordered_arg("UNIT_TEST_RESULT_FETCHER")
            .add_expected_arg("--debug")
            .add_expected_arg("--smtp_server", param="smtp.gmail.com")
            .add_expected_arg("--smtp_port", param="465")
            .add_expected_arg("--account_user", param="testMailUser")
            .add_expected_arg("--account_password", param="testMailPassword")
            .add_expected_arg("--sender", param=wrap_d("YARN unit test result fetcher"))
            .add_expected_arg("--recipients", param="yarn_eng_bp@cloudera.com")
            .add_expected_arg("--mode", param="jenkins_master")
            .add_expected_arg(
                "--testcase-filter",
                param="YARN:org.apache.hadoop.yarn "
                "MAPREDUCE:org.apache.hadoop.mapreduce "
                "HDFS:org.apache.hadoop.hdfs "
                "HADOOP_COMMON:org.apache.hadoop",
            )
            .add_expected_arg("--request-limit", param="999")
            .add_expected_arg("--num-builds", param="jenkins_examine_unlimited_builds")
            .add_expected_arg("--cache-type", param="google_drive")
            .with_command_type(CommandType.UNIT_TEST_RESULT_FETCHER)
        )

        expectations = [exp_command_1]
        CdswTestingCommons.verify_commands(self, expectations, cdsw_runner.executed_commands)

    def _get_expected_zip_latest_command_data_command(self, cmd_type: CommandType):
        exp_command_2 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg(self.yarn_dev_tools_script_path)
            .add_expected_ordered_arg("ZIP_LATEST_COMMAND_DATA")
            .add_expected_ordered_arg(cmd_type.name)
            .add_expected_arg("--debug")
            .add_expected_arg("--dest_dir", "/tmp")
            .add_expected_arg("--ignore-filetypes", "java js")
            .with_command_type(CommandType.ZIP_LATEST_COMMAND_DATA)
        )
        return exp_command_2

    def test_jira_umbrella_data_fetcher_e2e(self):
        cdsw_root_dir: str = self.cdsw_testing_commons.cdsw_root_dir
        config_file = FileUtils.find_files(
            cdsw_root_dir,
            find_type=FindResultType.FILES,
            regex="jira_umbrella_data_fetcher_.*",
            single_level=False,
            full_path_result=True,
            exclude_dirs=["yarndevtools-results"],
        )[0]

        umbrella_ids = " ".join(JIRA_UMBRELLA_FETCHER_UPSTREAM_UMBRELLA_IDS)
        self._set_env_vars_from_dict(
            {
                "BRANCHES": "branch-3.2 branch-3.3",
                "MAIL_ACC_USER": "testMailUser",
                "MAIL_ACC_PASSWORD": "testMailPassword",
                "UMBRELLA_IDS": f'"{umbrella_ids}"',
            }
        )

        args = self._create_args_for_specified_file(config_file, CommandType.JIRA_UMBRELLA_DATA_FETCHER, dry_run=True)
        cdsw_runner_config = CdswRunnerConfig(PARSER, args, config_reader=CdswConfigReaderAdapter())
        cdsw_runner = FakeCdswRunner(cdsw_runner_config)
        cdsw_runner.start()

        job_start_date = cdsw_runner.job_config.job_start_date()
        wrap_d = StringUtils.wrap_to_quotes
        sender = wrap_d("YARN upstream umbrella checker")
        subject1 = wrap_d(
            f"YARN Upstream umbrella checker report: "
            f"[UMBRELLA: {JIRA_UMBRELLA_FETCHER_UPSTREAM_UMBRELLA_IDS[0]} ([Umbrella] Support Flexible Auto Queue Creation in Capacity Scheduler), "
            f"start date: {job_start_date}]"
        )

        subject2 = wrap_d(
            f"YARN Upstream umbrella checker report: "
            f"[UMBRELLA: {JIRA_UMBRELLA_FETCHER_UPSTREAM_UMBRELLA_IDS[1]} ([Umbrella] Natively support GPU configuration/discovery/scheduling/isolation on YARN), "
            f"start date: {job_start_date}]"
        )

        exp_command_1 = self._get_expected_jira_umbrella_data_fetcher_main_command(
            JIRA_UMBRELLA_FETCHER_UPSTREAM_UMBRELLA_IDS[0]
        )
        exp_command_2 = self._get_expected_zip_latest_command_data_command(CommandType.JIRA_UMBRELLA_DATA_FETCHER)
        exp_command_3 = self._get_expected_send_latest_command_data_command(
            job_start_date, subject=subject1, sender=sender
        )
        exp_command_4 = self._get_expected_jira_umbrella_data_fetcher_main_command(
            JIRA_UMBRELLA_FETCHER_UPSTREAM_UMBRELLA_IDS[1]
        )
        exp_command_5 = self._get_expected_zip_latest_command_data_command(CommandType.JIRA_UMBRELLA_DATA_FETCHER)
        exp_command_6 = self._get_expected_send_latest_command_data_command(
            job_start_date, subject=subject2, sender=sender
        )

        expectations = [
            self.exp_command_clone_downstream_repos,
            self.exp_command_clone_upstream_repos,
            exp_command_1,
            exp_command_2,
            exp_command_3,
            exp_command_4,
            exp_command_5,
            exp_command_6,
        ]
        CdswTestingCommons.verify_commands(self, expectations, cdsw_runner.executed_commands)

    def test_unit_test_result_aggregator_e2e(self):
        cdsw_root_dir: str = self.cdsw_testing_commons.cdsw_root_dir
        config_file = FileUtils.find_files(
            cdsw_root_dir,
            find_type=FindResultType.FILES,
            regex="unit_test_result_aggregator_.*",
            single_level=False,
            full_path_result=True,
            exclude_dirs=["yarndevtools-results"],
        )[0]

        skip_aggregation_defaults_file = FileUtils.find_files(
            cdsw_root_dir,
            find_type=FindResultType.FILES,
            regex="skip_aggregation_defaults.*",
            single_level=False,
            full_path_result=True,
            exclude_dirs=["yarndevtools-results"],
        )[0]

        self._set_env_vars_from_dict(
            {
                "GSHEET_CLIENT_SECRET": "testGsheetClientSecret",
                "GSHEET_WORKSHEET": "testGsheetWorkSheet",
                "GSHEET_SPREADSHEET": "testGsheetSpreadSheet",
                "MAIL_ACC_USER": "testMailUser",
                "MAIL_ACC_PASSWORD": "testMailPassword",
                "MATCH_EXPRESSION": "YARN::org.apache.hadoop.yarn MR::org.apache.hadoop.mapreduce",
                "ABBREV_TC_PACKAGE": "org.apache.hadoop.yarn.server",
                "AGGREGATE_FILTERS": "CDPD-7.1.x CDPD-7.x",
                "GSHEET_COMPARE_WITH_JIRA_TABLE": GenericCdswConfigUtils.quote("testcases with jiras"),
                "SKIP_AGGREGATION_RESOURCE_FILE": skip_aggregation_defaults_file,
                "SKIP_AGGREGATION_RESOURCE_FILE_AUTO_DISCOVERY": "0",
                "REQUEST_LIMIT": "3000",
                "DEBUG_ENABLED": False,
            }
        )

        # TODO yarndevtoolsv2 DB: Add testcase for UNIT_TEST_RESULT_AGGREGATOR_DB
        args = self._create_args_for_specified_file(
            config_file, CommandType.UNIT_TEST_RESULT_AGGREGATOR_EMAIL, dry_run=True
        )
        cdsw_runner_config = CdswRunnerConfig(PARSER, args, config_reader=CdswConfigReaderAdapter())
        cdsw_runner = FakeCdswRunner(cdsw_runner_config)
        cdsw_runner.start()

        job_start_date = cdsw_runner.job_config.job_start_date()
        wrap_d = StringUtils.wrap_to_quotes
        sender = wrap_d("YARN unit test aggregator")
        subject = wrap_d(f"YARN unit test aggregator report [start date: {job_start_date}]")

        exp_command_1 = (
            CommandExpectations(self)
            .add_expected_ordered_arg(PYTHON3)
            .add_expected_ordered_arg(self.yarn_dev_tools_script_path)
            .add_expected_ordered_arg("UNIT_TEST_RESULT_AGGREGATOR_EMAIL")
            .add_expected_arg("--gsheet")
            .add_expected_arg("--gsheet-client-secret", "testGsheetClientSecret")
            .add_expected_arg("--gsheet-worksheet", "testGsheetWorkSheet")
            .add_expected_arg("--gsheet-spreadsheet", "testGsheetSpreadSheet")
            .add_expected_arg("--account-email", "testMailUser")
            .add_expected_arg("--request-limit", "3000")
            .add_expected_arg("--gmail-query", 'subject:"YARN Daily unit test report"')
            .add_expected_args("--match-expression", "YARN::org.apache.hadoop.yarn", "MR::org.apache.hadoop.mapreduce")
            .add_expected_arg(
                "--skip-lines-starting-with",
                '"Failed testcases:" '
                '"Failed testcases (filter: org.apache.hadoop.yarn):" '
                '"FILTER:" '
                '"Filter expression:" '
                '"Project: YARN, filter expression: org.apache.hadoop.yarn" '
                '"org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestLeafQueue.org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.TestLeafQueue" '
                '"org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.TestFSConfigToCSConfigConverter.org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.TestFSConfigToCSConfigConverter"',
            )
            .add_expected_arg("--summary-mode", "html")
            .add_expected_arg("--smart-subject-query")
            .add_expected_arg("--abbreviate-testcase-package", "org.apache.hadoop.yarn.server")
            .add_expected_args("--aggregate-filters", "CDPD-7.1.x", "CDPD-7.x")
            .add_expected_arg("--gsheet-compare-with-jira-table", '"testcases with jiras"')
            .with_command_type(CommandType.UNIT_TEST_RESULT_AGGREGATOR_EMAIL)
        )
        exp_command_2 = self._get_expected_zip_latest_command_data_command(
            CommandType.UNIT_TEST_RESULT_AGGREGATOR_EMAIL
        )
        exp_command_3 = self._get_expected_send_latest_command_data_command(
            job_start_date, subject=subject, sender=sender, email_file_from_zip="report-short.html"
        )
        expectations = [exp_command_1, exp_command_2, exp_command_3]
        CdswTestingCommons.verify_commands(self, expectations, cdsw_runner.executed_commands)

    def test_branch_comparator_e2e(self):
        cdsw_root_dir: str = self.cdsw_testing_commons.cdsw_root_dir
        config_file = FileUtils.find_files(
            cdsw_root_dir,
            find_type=FindResultType.FILES,
            regex="branch_comparator_.*",
            single_level=False,
            full_path_result=True,
            exclude_dirs=["yarndevtools-results"],
        )[0]

        self._set_env_vars_from_dict(
            {
                "MAIL_ACC_USER": "testMailUser",
                "MAIL_ACC_PASSWORD": "testMailPassword",
                "BRANCH_COMP_MASTER_BRANCH": "someMasterBranch",
                "BRANCH_COMP_FEATURE_BRANCH": "someFeatureBranch",
            }
        )

        args = self._create_args_for_specified_file(config_file, CommandType.BRANCH_COMPARATOR, dry_run=True)
        tmp_dir: tempfile.TemporaryDirectory = tempfile.TemporaryDirectory()
        tmp_dir_path = tmp_dir.name
        cdsw_runner_config = CdswRunnerConfig(
            PARSER, args, config_reader=CdswConfigReaderAdapter(), hadoop_cloudera_basedir=tmp_dir_path
        )
        cdsw_runner = FakeCdswRunner(cdsw_runner_config)
        cdsw_runner.start()

        job_start_date = cdsw_runner.job_config.job_start_date()
        wrap_d = StringUtils.wrap_to_quotes
        sender = wrap_d("YARN branch diff reporter")
        subject1 = wrap_d(f"YARN branch diff report [simple algorithm, start date: {job_start_date}]")
        subject2 = wrap_d(f"YARN branch diff report [grouped algorithm, start date: {job_start_date}]")

        exp_command_1_1 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg(self.yarn_dev_tools_script_path)
            .add_expected_ordered_arg("BRANCH_COMPARATOR")
            .add_expected_ordered_arg("simple")
            .add_expected_ordered_arg("someFeatureBranch")
            .add_expected_ordered_arg("someMasterBranch")
            .add_expected_arg("--debug")
            .add_expected_arg("--repo-type", "downstream")
            .add_expected_arg("--commit_author_exceptions", "rel-eng@cloudera.com")
            .with_command_type(CommandType.BRANCH_COMPARATOR)
        )
        exp_command_2_1 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg(self.yarn_dev_tools_script_path)
            .add_expected_ordered_arg("BRANCH_COMPARATOR")
            .add_expected_ordered_arg("grouped")
            .add_expected_ordered_arg("someFeatureBranch")
            .add_expected_ordered_arg("someMasterBranch")
            .add_expected_arg("--debug")
            .add_expected_arg("--repo-type", "downstream")
            .add_expected_arg("--commit_author_exceptions", "rel-eng@cloudera.com")
            .with_command_type(CommandType.BRANCH_COMPARATOR)
        )
        exp_command_1_2 = exp_command_2_2 = self._get_expected_zip_latest_command_data_command(
            CommandType.BRANCH_COMPARATOR
        )
        exp_command_1_3 = self._get_expected_send_latest_command_data_command(
            job_start_date,
            subject=subject1,
            sender=sender,
            email_file_from_zip="summary.html",
            command_data_filename=f"command_data_simple_{job_start_date}.zip",
        )

        exp_command_2_3 = self._get_expected_send_latest_command_data_command(
            job_start_date,
            subject=subject2,
            sender=sender,
            email_file_from_zip="summary.html",
            command_data_filename=f"command_data_grouped_{job_start_date}.zip",
        )
        expectations = [
            self.exp_command_clone_downstream_repos,
            exp_command_1_1,
            exp_command_1_2,
            exp_command_1_3,
            exp_command_2_1,
            exp_command_2_2,
            exp_command_2_3,
        ]
        CdswTestingCommons.verify_commands(self, expectations, cdsw_runner.executed_commands)

    def _get_expected_send_latest_command_data_command(
        self, job_start_date, subject, sender, email_file_from_zip="summary.html", command_data_filename=None
    ):
        if not command_data_filename:
            command_data_filename = f"command_data_{job_start_date}.zip"
        wrap_d = StringUtils.wrap_to_quotes
        wrap_s = StringUtils.wrap_to_single_quotes
        expected_html_link = wrap_s(f'<a href="dummy_link">Command data file: {command_data_filename}</a>')
        exp_command_3 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg(self.yarn_dev_tools_script_path)
            .add_expected_ordered_arg("SEND_LATEST_COMMAND_DATA")
            .add_expected_arg("--debug")
            .add_expected_arg("--smtp_server", wrap_d("smtp.gmail.com"))
            .add_expected_arg("--smtp_port", "465")
            .add_expected_arg("--account_user", wrap_d("testMailUser"))
            .add_expected_arg("--account_password", wrap_d("testMailPassword"))
            .add_expected_arg("--subject", subject)
            .add_expected_arg("--sender", sender)
            .add_expected_arg("--recipients", wrap_d("yarn_eng_bp@cloudera.com"))
            .add_expected_arg("--attachment-filename", command_data_filename)
            .add_expected_arg("--file-as-email-body-from-zip", email_file_from_zip)
            .add_expected_arg("--prepend_email_body_with_text", expected_html_link)
            .add_expected_arg("--send-attachment")
            .with_command_type(CommandType.SEND_LATEST_COMMAND_DATA)
        )
        return exp_command_3

    def _get_expected_jira_umbrella_data_fetcher_main_command(self, umbrella_id: str):
        exp_command_1 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg(self.yarn_dev_tools_script_path)
            .add_expected_ordered_arg("JIRA_UMBRELLA_DATA_FETCHER")
            .add_expected_arg("--debug")
            .add_expected_arg("--branches", "origin/CDH-7.1-maint origin/cdpd-master origin/CDH-7.1.6.x")
            .add_expected_arg("--force-mode")
            .add_expected_arg("--ignore-changes")
            .add_expected_arg(umbrella_id)
            .with_command_type(CommandType.JIRA_UMBRELLA_DATA_FETCHER)
        )
        return exp_command_1
