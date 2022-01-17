import os
import unittest
from typing import Dict

from pythoncommons.file_utils import FileUtils, FindResultType
from pythoncommons.string_utils import StringUtils

from tests.cdsw.common.testutils.cdsw_testing_common import CdswTestingCommons, CommandExpectations
from tests.test_utilities import Object
from yarndevtools.cdsw.common_python.cdsw_common import CommonFiles
from yarndevtools.cdsw.common_python.cdsw_runner import NewCdswRunnerConfig, NewCdswConfigReaderAdapter, NewCdswRunner
from yarndevtools.common.shared_command_utils import CommandType

PARSER = None
SETUP_RESULT = None
CDSW_RUNNER_SCRIPT_PATH = None


class TestNewCdswRunnerJobsE2E(unittest.TestCase):
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

    def setUp(self) -> None:
        CommonFiles.YARN_DEV_TOOLS_SCRIPT = "yarndevtools.py"
        self.cdsw_testing_commons = CdswTestingCommons()

    def tearDown(self) -> None:
        self._clear_env_vars()

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
            os.environ[k] = v

    def test_reviewsync_e2e(self):
        cdsw_root_dir: str = self.cdsw_testing_commons.cdsw_root_dir
        config_file = FileUtils.find_files(
            cdsw_root_dir,
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
        cdsw_runner_config = NewCdswRunnerConfig(PARSER, args, config_reader=NewCdswConfigReaderAdapter())
        cdsw_runner = NewCdswRunner(cdsw_runner_config)
        cdsw_runner.start(SETUP_RESULT, CDSW_RUNNER_SCRIPT_PATH)

        exp_command_1 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg("yarndevtools.py")
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
        )

        exp_command_2 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg("yarndevtools.py")
            .add_expected_ordered_arg("ZIP_LATEST_COMMAND_DATA")
            .add_expected_ordered_arg("REVIEWSYNC")
            .add_expected_arg("--debug")
            .add_expected_arg("--dest_dir", "/tmp")
            .add_expected_arg("--ignore-filetypes", "java js")
        )

        job_start_date = cdsw_runner.job_config.job_start_date()

        wrap_d = StringUtils.wrap_to_quotes
        wrap_s = StringUtils.wrap_to_single_quotes
        expected_html_link = wrap_s(f'<a href="dummy_link">Command data file: command_data_{job_start_date}.zip</a>')
        exp_command_3 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg("yarndevtools.py")
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
            .add_expected_arg("--file-as-email-body-from-zip", "report-short.html")
            .add_expected_arg("--prepend_email_body_with_text", expected_html_link)
            .add_expected_arg("--send-attachment")
        )

        expectations = [exp_command_1, exp_command_2, exp_command_3]
        CdswTestingCommons.assert_commands(self, expectations, cdsw_runner.executed_commands)

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
        cdsw_runner_config = NewCdswRunnerConfig(PARSER, args, config_reader=NewCdswConfigReaderAdapter())
        cdsw_runner = NewCdswRunner(cdsw_runner_config)
        cdsw_runner.start(SETUP_RESULT, CDSW_RUNNER_SCRIPT_PATH)

        exp_command_1 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg("yarndevtools.py")
            .add_expected_ordered_arg("REVIEW_SHEET_BACKPORT_UPDATER")
            .add_expected_arg("--gsheet")
            .add_expected_arg("--debug")
            .add_expected_arg("--gsheet-client-secret", "testGsheetClientSecret")
            .add_expected_arg("--gsheet-worksheet", "testGsheetWorkSheet")
            .add_expected_arg("--gsheet-spreadsheet", "testGsheetSpreadSheet")
            .add_expected_arg("--gsheet-jira-column", "testGsheetJiraColumn")
            .add_expected_arg("--gsheet-update-date-column", "testGsheetUpdateDateColumn")
            .add_expected_arg("--gsheet-status-info-column", "testGsheetStatusInfoColumn")
            .add_expected_arg("--branches", "branch-3.2 branch-3.3")
        )

        exp_command_2 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg("yarndevtools.py")
            .add_expected_ordered_arg("ZIP_LATEST_COMMAND_DATA")
            .add_expected_ordered_arg("REVIEW_SHEET_BACKPORT_UPDATER")
            .add_expected_arg("--debug")
            .add_expected_arg("--dest_dir", "/tmp")
            .add_expected_arg("--ignore-filetypes", "java js")
        )

        job_start_date = cdsw_runner.job_config.job_start_date()

        wrap_d = StringUtils.wrap_to_quotes
        wrap_s = StringUtils.wrap_to_single_quotes
        expected_html_link = wrap_s(f'<a href="dummy_link">Command data file: command_data_{job_start_date}.zip</a>')
        exp_command_3 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg("yarndevtools.py")
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
            .add_expected_arg("--file-as-email-body-from-zip", "report-short.html")
            .add_expected_arg("--prepend_email_body_with_text", expected_html_link)
            .add_expected_arg("--send-attachment")
        )

        expectations = [exp_command_1, exp_command_2, exp_command_3]
        CdswTestingCommons.assert_commands(self, expectations, cdsw_runner.executed_commands)

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

        args = self._create_args_for_specified_file(config_file, CommandType.UNIT_TEST_RESULT_FETCHER, dry_run=True)
        cdsw_runner_config = NewCdswRunnerConfig(PARSER, args, config_reader=NewCdswConfigReaderAdapter())
        cdsw_runner = NewCdswRunner(cdsw_runner_config)
        cdsw_runner.start(SETUP_RESULT, CDSW_RUNNER_SCRIPT_PATH)

        exp_command_1 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg("yarndevtools.py")
            .add_expected_ordered_arg("UNIT_TEST_RESULT_FETCHER")
            .add_expected_arg("--debug")
            .add_expected_arg("--smtp_server", param="smtp.gmail.com")
            .add_expected_arg("--smtp_port", param="465")
            .add_expected_arg("--account_user", param="testMailUser")
            .add_expected_arg("--account_password", param="testMailPassword")
            .add_expected_arg("--sender", param="YARN unit test result fetcher")
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
        )

        exp_command_2 = (
            CommandExpectations(self)
            .add_expected_ordered_arg("python3")
            .add_expected_ordered_arg("yarndevtools.py")
            .add_expected_ordered_arg("ZIP_LATEST_COMMAND_DATA")
            .add_expected_ordered_arg("UNIT_TEST_RESULT_FETCHER")
            .add_expected_arg("--debug")
            .add_expected_arg("--dest_dir", "/tmp")
            .add_expected_arg("--ignore-filetypes", "java js")
        )

        expectations = [exp_command_1, exp_command_2]
        CdswTestingCommons.assert_commands(self, expectations, cdsw_runner.executed_commands)
