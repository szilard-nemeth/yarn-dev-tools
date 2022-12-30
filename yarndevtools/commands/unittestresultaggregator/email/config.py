import logging
from typing import List

from googleapiwrapper.google_sheet import GSheetOptions
from pythoncommons.file_utils import FileUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils

from yarndevtools.commands.unittestresultaggregator.common.model import TestCaseFilterDefinitions, TestCaseFilter
from yarndevtools.commands.unittestresultaggregator.email.processor import DEFAULT_LINE_SEP
from yarndevtools.common.db import MongoDbConfig
from yarndevtools.commands.unittestresultaggregator.constants import (
    OperationMode,
    ExecutionMode,
    VALID_OPERATION_MODES,
    AGGREGATED_WS_POSTFIX,
    MATCHTYPE_ALL_POSTFIX,
)


LOG = logging.getLogger(__name__)


class UnitTestResultAggregatorEmailConfig:
    def __init__(self, args, output_dir):
        self.gmail_query = args.gmail_query
        self.smart_subject_query = args.smart_subject_query
        self.request_limit = getattr(args, "request_limit", 1000000)
        self.account_email: str = args.account_email
        self.skip_lines_starting_with: List[str] = getattr(args, "skip_lines_starting_with", [])
        self.content_line_sep = getattr(args, "email_content_line_separator", DEFAULT_LINE_SEP)
        self.truncate_subject_with: str = getattr(args, "truncate_subject", None)
        self.email_cache_dir = FileUtils.join_path(output_dir, "email_cache")


class UnitTestResultAggregatorConfig:
    def __init__(self, parser, args, output_dir: str):
        # Minimal configs for validation
        self.execution_mode = getattr(args, "execution_mode", None)
        self._set_operation_mode(args)
        UnitTestResultAggregatorConfig._validate_args(parser, args, self.operation_mode, self.execution_mode)

        self.email = UnitTestResultAggregatorEmailConfig(args, output_dir)

        # GSheet
        self.testcase_filter_defs = TestCaseFilterDefinitions(
            TestCaseFilterDefinitions.convert_raw_match_expressions_to_objs(getattr(args, "match_expression", None)),
            self._get_attribute(args, "aggregate_filters", default=[]),
        )
        if self.operation_mode == OperationMode.GSHEET:
            self.gsheet = UnitTestResultAggregatorGSheetConfig(args, self.testcase_filter_defs)

        # Common args
        self.console_mode = getattr(args, "console mode", False)
        self.abbrev_tc_package: str = getattr(args, "abbrev_testcase_package", None)
        self.summary_mode = args.summary_mode
        self.output_dir = output_dir
        self.session_dir = ProjectUtils.get_session_dir_under_child_dir(FileUtils.basename(output_dir))
        self._full_cmd: str = OsUtils.determine_full_command_filtered(filter_password=True)

        # Derived fields for execution modes
        self._set_derived_fields_for_exec_mode(args)

    @staticmethod
    def _get_attribute(args, attr_name, default=None):
        val = getattr(args, attr_name)
        if not val:
            return default
        return val

    @staticmethod
    def _validate_args(parser, args, operation_mode, execution_mode):
        if args.gsheet and (
            args.gsheet_client_secret is None or args.gsheet_spreadsheet is None or args.gsheet_worksheet is None
        ):
            parser.error(
                "--gsheet requires the following arguments: "
                "--gsheet-client-secret, --gsheet-spreadsheet and --gsheet-worksheet."
            )

        if operation_mode not in VALID_OPERATION_MODES:
            raise ValueError(
                f"Unknown state! "
                f"Operation mode should be any of {VALID_OPERATION_MODES}, but it is set to: {operation_mode}"
            )

        if hasattr(args, "gmail_credentials_file"):
            FileUtils.ensure_file_exists(args.gmail_credentials_file)

        if not execution_mode:
            raise ValueError("Execution mode should be specified!")

    def _set_derived_fields_for_exec_mode(self, args):
        self.should_use_db = self.execution_mode in (ExecutionMode.DB_ONLY, ExecutionMode.DB_AND_EMAIL)
        self.should_fetch_mails = self.execution_mode in (ExecutionMode.EMAIL_ONLY, ExecutionMode.DB_AND_EMAIL)
        self.should_store_email_content_to_db = self.execution_mode in (
            ExecutionMode.DB_AND_EMAIL,
            ExecutionMode.DB_ONLY,
        )
        self.should_generate_summary = self.execution_mode == ExecutionMode.EMAIL_ONLY
        if self.should_use_db:
            self.mongo_config = MongoDbConfig(args)

    def __str__(self):
        return (
            f"Full command was: {self._full_cmd}\n"
            f"Output dir: {self.output_dir}\n"
            f"Account email: {self.account_email}\n"
            f"Email cache dir: {self.email_cache_dir}\n"
            f"Session dir: {self.session_dir}\n"
            f"Console mode: {self.console_mode}\n"
            f"Gmail query: {self.gmail_query}\n"
            f"Smart subject query: {self.smart_subject_query}\n"
            f"Testcase filters: {self.testcase_filter_defs}\n"
            f"Email line separator: {self.email_content_line_sep}\n"
            f"Request limit: {self.request_limit}\n"
            f"Operation mode: {self.operation_mode}\n"
            f"Skip lines starting with: {self.skip_lines_starting_with}\n"
            f"Truncate subject with: {self.truncate_origin_with}\n"
            f"Abbreviate testcase package: {self.abbrev_tc_package}\n"
            f"Summary mode: {self.summary_mode}\n"
        )

    @property
    def account_email(self):
        return self.email.account_email

    @property
    def email_cache_dir(self):
        return self.email.email_cache_dir

    @property
    def gmail_query(self):
        return self.email.gmail_query

    @property
    def smart_subject_query(self):
        return self.email.smart_subject_query

    @property
    def truncate_origin_with(self):
        return self.email.truncate_subject_with

    @property
    def email_content_line_sep(self):
        return self.email.content_line_sep

    @property
    def request_limit(self):
        return self.email.request_limit

    @property
    def skip_lines_starting_with(self):
        return self.email.skip_lines_starting_with

    @property
    def gsheet_options(self):
        return self.gsheet.gsheet_options

    @property
    def gsheet_jira_table(self):
        return self.gsheet.gsheet_jira_table

    def _set_operation_mode(self, args):
        if args.do_print:
            self.operation_mode = OperationMode.PRINT
        elif args.gsheet:
            self.operation_mode = OperationMode.GSHEET


class UnitTestResultAggregatorGSheetConfig:
    def __init__(self, args, testcase_filter_defs):
        self.gsheet_options = GSheetOptions(args.gsheet_client_secret, args.gsheet_spreadsheet, worksheet=None)
        self.gsheet_jira_table = getattr(args, "gsheet_compare_with_jira_table", None)

        worksheet_names: List[str] = [self.get_worksheet_name(tcf) for tcf in testcase_filter_defs.ALL_VALID_FILTERS]
        LOG.info(
            f"Adding worksheets to {self.gsheet_options.__class__.__name__}. "
            f"Generated worksheet names: {worksheet_names}"
        )
        for worksheet_name in worksheet_names:
            self.gsheet_options.add_worksheet(worksheet_name)

    @staticmethod
    def get_worksheet_name(tcf: TestCaseFilter):
        worksheet_name: str = f"{tcf.match_expr.alias}"
        if tcf.aggr_filter:
            worksheet_name += f"_{tcf.aggr_filter.val}_{AGGREGATED_WS_POSTFIX}"
        elif tcf.aggregate:
            worksheet_name += f"_{AGGREGATED_WS_POSTFIX}"
        else:
            worksheet_name += f"_{MATCHTYPE_ALL_POSTFIX}"
        return f"{worksheet_name}"
