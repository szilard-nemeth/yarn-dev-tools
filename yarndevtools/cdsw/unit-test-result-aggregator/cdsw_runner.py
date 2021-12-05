#!/usr/bin/env python3

import logging
import os
from enum import Enum
from typing import List

from googleapiwrapper.google_drive import DriveApiFile
from pythoncommons.file_utils import FileUtils
from pythoncommons.os_utils import OsUtils

from yarndevtools.argparser import CommandType
from yarndevtools.cdsw.common_python.cdsw_common import (
    CdswRunnerBase,
    CdswSetup,
    CommonDirs,
    SKIP_AGGREGATION_DEFAULTS_FILENAME,
    CdswSetupResult,
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
    SKIP_AGGREGATION_RESOURCE_FILE_AUTO_DISCOVERY = "SKIP_AGGREGATION_RESOURCE_FILE_AUTO_DISCOVERY"
    GSHEET_COMPARE_WITH_JIRA_TABLE = "GSHEET_COMPARE_WITH_JIRA_TABLE"


class CdswRunner(CdswRunnerBase):
    def start(self, setup_result: CdswSetupResult, cdsw_runner_script_path: str):
        self.start_common(setup_result, cdsw_runner_script_path)
        self.run_test_result_aggregator_and_send_mail()

    def run_test_result_aggregator_and_send_mail(self):
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
            skip_lines_starting_with=self._determine_lines_to_skip(),
        )

        cmd_type = CommandType.UNIT_TEST_RESULT_AGGREGATOR
        self.run_zipper(cmd_type, debug=True)

        sender = "YARN unit test aggregator"
        subject = f"YARN unit test aggregator report [start date: {self.start_date_str}]"
        command_data_filename: str = f"command_data_{self.start_date_str}.zip"
        drive_api_file: DriveApiFile = self.upload_command_data_to_drive(cmd_type, command_data_filename)
        link_text = f'<a href="{drive_api_file.link}">Command data file: {command_data_filename}</a>'
        self.send_latest_command_data_in_email(
            sender=sender,
            subject=subject,
            attachment_filename=command_data_filename,
            email_body_file=REPORT_FILE_SHORT_HTML,
            prepend_text_to_email_body=link_text,
        )

    @staticmethod
    def _determine_lines_to_skip() -> List[str]:
        skip_lines_starting_with: List[str] = DEFAULT_SKIP_LINES_STARTING_WITH
        # If env var "SKIP_AGGREGATION_RESOURCE_FILE" is specified, try to read file
        # The file takes precedence over the default list of DEFAULT_SKIP_LINES_STARTING_WITH
        skip_aggregation_res_file = OsUtils.get_env_value(
            UnitTestResultAggregatorOptionalEnvVar.SKIP_AGGREGATION_RESOURCE_FILE.value
        )
        skip_aggregation_res_file_auto_discovery = OsUtils.get_env_value(
            UnitTestResultAggregatorOptionalEnvVar.SKIP_AGGREGATION_RESOURCE_FILE_AUTO_DISCOVERY.value
        )
        LOG.info(
            "Value of env var '%s': %s",
            UnitTestResultAggregatorOptionalEnvVar.SKIP_AGGREGATION_RESOURCE_FILE_AUTO_DISCOVERY.value,
            skip_aggregation_res_file_auto_discovery,
        )

        found_with_auto_discovery: str = None
        if skip_aggregation_res_file_auto_discovery:
            results = FileUtils.search_files(CommonDirs.YARN_DEV_TOOLS_MODULE_ROOT, SKIP_AGGREGATION_DEFAULTS_FILENAME)
            if not results:
                LOG.warning(
                    "Skip aggregation resource file auto-discovery is enabled, "
                    "but failed to find file '%s' from base directory '%s'.",
                    SKIP_AGGREGATION_DEFAULTS_FILENAME,
                    CommonDirs.YARN_DEV_TOOLS_MODULE_ROOT,
                )
            elif len(results) > 1:
                LOG.warning(
                    "Skip aggregation resource file auto-discovery is enabled, "
                    "but but found multiple files from base directory '%s'. Found files: %s",
                    SKIP_AGGREGATION_DEFAULTS_FILENAME,
                    CommonDirs.YARN_DEV_TOOLS_MODULE_ROOT,
                    results,
                )
            else:
                found_with_auto_discovery = results[0]
        if found_with_auto_discovery:
            LOG.info("Found Skip aggregation resource file with auto-discovery: %s", found_with_auto_discovery)
            return FileUtils.read_file_to_list(found_with_auto_discovery)
        elif skip_aggregation_res_file:
            LOG.info("Trying to check specified skip aggregation resource file: %s", skip_aggregation_res_file)
            FileUtils.ensure_is_file(skip_aggregation_res_file)
            return FileUtils.read_file_to_list(skip_aggregation_res_file)
        return skip_lines_starting_with

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
            f"{CommandType.UNIT_TEST_RESULT_AGGREGATOR.name} "
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
    setup_result: CdswSetupResult = CdswSetup.initial_setup(mandatory_env_vars=mandatory_env_vars)
    runner = CdswRunner()
    runner.start(setup_result, CdswRunnerBase.get_filename())
