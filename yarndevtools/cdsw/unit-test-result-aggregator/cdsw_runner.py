import logging
from enum import Enum
from typing import List

from pythoncommons.file_utils import FileUtils
from pythoncommons.os_utils import OsUtils

from yarndevtools.argparser import CommandType
from yarndevtools.cdsw.common_python.cdsw_common import (
    CdswRunnerBase,
    CdswSetup,
    YARN_DEV_TOOLS_ROOT_DIR,
)
from yarndevtools.cdsw.common_python.constants import CdswEnvVar
from yarndevtools.constants import REPORT_FILE_SHORT_HTML

LOG = logging.getLogger(__name__)
CMD_LOG = logging.getLogger(__name__)

DEFAULT_GMAIL_QUERY = 'subject:"YARN Daily unit test report"'
DEFAULT_TRUNCATE_SUBJECT = "YARN Daily unit test report: Failed tests with build: "
DEFAULT_SKIP_LINES_STARTING_WITH = ["Failed testcases:", "Failed testcases (", "FILTER:", "Filter expression: "]


class UnitTestResultAggregatorEnvVar(Enum):
    GSHEET_CLIENT_SECRET = "GSHEET_CLIENT_SECRET"
    GSHEET_SPREADHSHEET = "GSHEET_SPREADHSHEET"
    GSHEET_WORKSHEET = "GSHEET_WORKSHEET"
    REQUEST_LIMIT = "REQUEST_LIMIT"
    MATCH_EXPRESSION = "MATCH_EXPRESSION"


class UnitTestResultAggregatorOptionalEnvVar(Enum):
    ABBREV_TC_PACKAGE = "ABBREV_TC_PACKAGE"
    AGGREGATE_FILTERS = "AGGREGATE_FILTERS"
    SKIP_AGGREGATION_RESOURCE_FILE = "SKIP_AGGREGATION_RESOURCE_FILE"
    GSHEET_COMPARE_WITH_JIRA_TABLE = "GSHEET_COMPARE_WITH_JIRA_TABLE"


class CdswRunner(CdswRunnerBase):
    def start(self, basedir):
        LOG.info("Starting CDSW runner...")
        self.run_test_result_aggregator_and_send_mail()

    def run_test_result_aggregator_and_send_mail(self):
        skip_lines_starting_with: List[str] = DEFAULT_SKIP_LINES_STARTING_WITH

        # If env var "SKIP_AGGREGATION_RESOURCE_FILE" is specified, try to read file
        # The file takes precedence over the default list of DEFAULT_SKIP_LINES_STARTING_WITH
        skip_aggregation_res_file = OsUtils.get_env_value(
            UnitTestResultAggregatorOptionalEnvVar.SKIP_AGGREGATION_RESOURCE_FILE.value
        )
        if skip_aggregation_res_file:
            FileUtils.ensure_is_file(skip_aggregation_res_file)
            skip_lines_starting_with = FileUtils.read_file_to_list(skip_aggregation_res_file)

        self._run_aggregator(
            exec_mode="gsheet",
            gsheet_client_secret=OsUtils.get_env_value(UnitTestResultAggregatorEnvVar.GSHEET_CLIENT_SECRET.value),
            gsheet_spreadsheet=OsUtils.get_env_value(UnitTestResultAggregatorEnvVar.GSHEET_SPREADHSHEET.value),
            gsheet_worksheet=OsUtils.get_env_value(UnitTestResultAggregatorEnvVar.GSHEET_WORKSHEET.value),
            account_email=OsUtils.get_env_value(CdswEnvVar.MAIL_ACC_USER.value),
            request_limit=OsUtils.get_env_value(UnitTestResultAggregatorEnvVar.REQUEST_LIMIT.value),
            match_expression=OsUtils.get_env_value(UnitTestResultAggregatorEnvVar.MATCH_EXPRESSION.value),
            abbreviate_tc_package=OsUtils.get_env_value(UnitTestResultAggregatorOptionalEnvVar.ABBREV_TC_PACKAGE.value),
            aggregate_filters=OsUtils.get_env_value(UnitTestResultAggregatorOptionalEnvVar.AGGREGATE_FILTERS.value),
            gsheet_compare_with_jira_table=OsUtils.get_env_value(
                UnitTestResultAggregatorOptionalEnvVar.GSHEET_COMPARE_WITH_JIRA_TABLE.value
            ),
            skip_lines_starting_with=skip_lines_starting_with,
        )

        self.run_zipper(CommandType.UNIT_TEST_RESULT_AGGREGATOR, debug=True)

        date_str = self.current_date_formatted()
        sender = "YARN unit test aggregator"
        subject = f"YARN unit test aggregator report [start date: {date_str}]"
        attachment_fnname: str = f"command_data_{date_str}.zip"
        self.send_latest_command_data_in_email(
            sender=sender,
            subject=subject,
            attachment_filename=attachment_fnname,
            email_body_file=REPORT_FILE_SHORT_HTML,
        )

    def _run_aggregator(
        self,
        exec_mode,
        gsheet_client_secret,
        gsheet_spreadsheet,
        gsheet_worksheet,
        account_email,
        request_limit,
        match_expression,
        gmail_query=DEFAULT_GMAIL_QUERY,
        skip_lines_starting_with: List[str] = None,
        debug=True,
        smart_subject_query=True,
        truncate_subject=None,
        abbreviate_tc_package=None,
        summary_mode="html",
        aggregate_filters=None,
        gsheet_compare_with_jira_table=None,
    ):
        if skip_lines_starting_with is None:
            skip_lines_starting_with = DEFAULT_SKIP_LINES_STARTING_WITH
        if not truncate_subject:
            truncate_subject = DEFAULT_TRUNCATE_SUBJECT
        if exec_mode != "print" and exec_mode != "gsheet":
            raise ValueError(f"Invalid execution mode detected. Valid execution modes are: {['print', 'gsheet']}")

        debug = "--debug" if debug else ""
        smart_subject_query = "--smart-subject-query" if smart_subject_query else ""
        abbreviate_tc_package = self._get_cli_switch_value("--abbreviate-testcase-package", abbreviate_tc_package)
        aggregate_filters = self._get_cli_switch_value("--aggregate-filters", aggregate_filters)
        gsheet_compare_with_jira_table = self._get_cli_switch_value(
            "--ghseet-compare-with-jira-table", gsheet_compare_with_jira_table, quote=True
        )
        skip_lines_starting_with_cli = self._get_cli_switch_value(
            "--skip-lines-starting-with", " ".join(f'"{w}"' for w in skip_lines_starting_with)
        )
        LOG.info(f"Locals: {locals()}")
        self.execute_yarndevtools_script(
            f"{debug} "
            f"{CommandType.UNIT_TEST_RESULT_AGGREGATOR.val} "
            f"--{exec_mode} "
            f"--gsheet-client-secret {gsheet_client_secret} "
            f"--gsheet-spreadsheet {gsheet_spreadsheet} "
            f"--gsheet-worksheet {gsheet_worksheet} "
            f"--account-email {account_email} "
            f"--request-limit {request_limit} "
            f"--gmail-query {gmail_query} "
            f"--match-expression {match_expression} "
            f"{skip_lines_starting_with_cli} "
            f"--summary-mode {summary_mode} "
            f"{smart_subject_query} "
            f"{abbreviate_tc_package} "
            f"{aggregate_filters} "
            f"{gsheet_compare_with_jira_table} "
        )

    @staticmethod
    def _get_cli_switch_value(switch_name, val, quote=False):
        if not val:
            return ""
        if quote:
            val = '"' + val + '"'
        return f"{switch_name} {val}"


if __name__ == "__main__":
    mandatory_env_vars = [CdswEnvVar.MAIL_ACC_USER.value, CdswEnvVar.MAIL_ACC_PASSWORD.value] + [
        e.value for e in UnitTestResultAggregatorEnvVar
    ]
    basedir = CdswSetup.initial_setup(mandatory_env_vars=mandatory_env_vars)
    LOG.info(f"YARN Dev tools mirror root dir: {YARN_DEV_TOOLS_ROOT_DIR}")
    runner = CdswRunner()
    runner.start(basedir)
