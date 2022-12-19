import datetime
from collections import defaultdict
from dataclasses import dataclass
from pprint import pformat
from typing import List, Callable, Dict, Tuple

from googleapiwrapper.common import ServiceType
from googleapiwrapper.gmail_api import GmailWrapper, ThreadQueryResults
from googleapiwrapper.gmail_domain import GmailMessage
from googleapiwrapper.google_auth import GoogleApiAuthorizer
from googleapiwrapper.google_sheet import GSheetOptions, GSheetWrapper
from pythoncommons.file_utils import FileUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils
from pythoncommons.string_utils import RegexUtils, auto_str

from yarndevtools.cdsw.constants import SECRET_PROJECTS_DIR
from yarndevtools.commands.unittestresultaggregator.common import (
    OperationMode,
    VALID_OPERATION_MODES,
    AGGREGATED_WS_POSTFIX,
    MATCHTYPE_ALL_POSTFIX,
    SummaryMode,
    KnownTestFailures,
    FinalAggregationResults,
    MATCH_ALL_LINES_EXPRESSION,
    get_key_by_testcase_filter,
    TestFailureComparison,
    LatestTestFailures,
    AggregatedTestFailures,
    KnownTestFailureChecker,
)
from yarndevtools.commands.unittestresultaggregator.common_tmp.model import (
    MatchExpression,
    BuildComparisonResult,
    FailedTestCaseAggregated,
    TestCaseFilter,
    TestCaseFilters,
    FailedTestCaseAbs,
    FailedTestCase,
    FailedTestCaseFactory,
)
from yarndevtools.commands_common import ArgumentParserUtils, GSheetArguments
from yarndevtools.common.shared_command_utils import CommandType

import logging

LOG = logging.getLogger(__name__)

SUBJECT = "subject:"
DEFAULT_LINE_SEP = "\\r\\n"


class EmailBasedAggregationResults:
    # TODO yarndevtoolsv2: consider extracting common aggregation logic from this class / or create abstraction layer?
    def __init__(self, testcase_filters: TestCaseFilters, known_failures: KnownTestFailures):
        self._match_all_lines: bool = self._should_match_all_lines(testcase_filters)
        self._testcase_filters: TestCaseFilters = testcase_filters
        self._known_failures: KnownTestFailures = known_failures
        self._aggregation_results: FinalAggregationResults = FinalAggregationResults(
            self._testcase_filters.ALL_VALID_FILTERS
        )

        # This is a temporary dict - usually for a context of a message
        # TODO yarndevtoolsv2: Can we get rid of str key altogether? (_get_matched_lines_key vs. TestCaseFilter)
        self._matched_lines_dict: Dict[str, List[str]] = {}
        self._str_key_to_testcase_filter: Dict[str, TestCaseFilter] = {}

    @staticmethod
    def _should_match_all_lines(testcase_filters):
        match_all_lines: bool = testcase_filters.match_all_lines()
        LOG.info(
            "**Matching all lines"
            if match_all_lines
            else f"**Matching lines with regex pattern: {testcase_filters.match_expressions}"
        )
        return match_all_lines

    def start_new_context(self):
        # Prepare matched_lines dict with all required empty-lists for ALL filters
        self._matched_lines_dict = defaultdict(list)
        filters: List[TestCaseFilter] = self._testcase_filters.ALL_VALID_FILTERS
        for tcf in filters:
            self._matched_lines_dict[self._get_matched_lines_key(tcf)] = []

        # Do sanity check
        generated_keys = [self._get_matched_lines_key(tcf) for tcf in filters]
        unique_keys = set(generated_keys)
        if len(filters) != len(unique_keys):
            raise ValueError(
                "Mismatch in number of testcase filter objects and generated keys. "
                f"Filters: {filters}, "
                f"Generated keys: {generated_keys}, "
                f"Unique keys: {unique_keys}."
            )

    def match_line(self, line, mail_subject: str):
        matches_any_pattern, matched_expression = self._does_line_match_any_match_expression(line, mail_subject)
        if self._match_all_lines or matches_any_pattern:
            self._matched_lines_dict[MATCHTYPE_ALL_POSTFIX].append(line)
            self._add_match_to_matched_lines_dict(line, matched_expression, aggregate_values=[True, False])

            for aggr_filter in self._testcase_filters.aggregate_filters:
                if aggr_filter.val in mail_subject:
                    LOG.debug(
                        f"Found matching email subject for aggregation filter '{aggr_filter}': "
                        f"Subject: {mail_subject}"
                    )
                    tcf = TestCaseFilter(matched_expression, aggr_filter)
                    self._matched_lines_dict[self._get_matched_lines_key(tcf)].append(line)

    def _add_match_to_matched_lines_dict(self, line, matched_expression, aggregate_values: List[bool]):
        for aggr_value in aggregate_values:
            tcf = TestCaseFilter(matched_expression, aggr_filter=None, aggregate=aggr_value)
            self._matched_lines_dict[self._get_matched_lines_key(tcf)].append(line)

    def _does_line_match_any_match_expression(self, line, mail_subject: str) -> Tuple[bool, MatchExpression or None]:
        for match_expression in self._testcase_filters.match_expressions:
            if RegexUtils.ensure_matches_pattern(line, match_expression.pattern):
                LOG.debug(f"Matched line: {line} [Mail subject: {mail_subject}]")
                return True, match_expression
        LOG.debug(f"Line did not match for any pattern: {line}")
        # TODO in strict mode, unmatching lines should not be allowed
        return False, None

    def _get_matched_lines_key(self, tcf: TestCaseFilter) -> str:
        if tcf.match_expr == MATCH_ALL_LINES_EXPRESSION:
            key = MATCHTYPE_ALL_POSTFIX + f"_{AGGREGATED_WS_POSTFIX}" if tcf.aggregate else MATCHTYPE_ALL_POSTFIX
            self._str_key_to_testcase_filter[key] = TestCaseFilter(MATCH_ALL_LINES_EXPRESSION, None)
            return key
        key = get_key_by_testcase_filter(tcf)
        if key not in self._str_key_to_testcase_filter:
            self._str_key_to_testcase_filter[key] = tcf
        return key

    def finish_context(self, message: GmailMessage):
        LOG.info("Finishing context...")
        LOG.debug(f"Keys of of matched lines: {self._matched_lines_dict.keys()}")

        for key, matched_lines in self._matched_lines_dict.items():
            if not matched_lines:
                continue
            tcf: TestCaseFilter = self._str_key_to_testcase_filter[key]
            for matched_line in matched_lines:
                email_meta = EmailMetaData(message.msg_id, message.thread_id, message.subject, message.date)
                failed_testcase = FailedTestCaseFactory.create_from_email(matched_line, email_meta)
                self._aggregation_results.add_failure(tcf, failed_testcase)

        self._aggregation_results.print_keys()
        # Make sure temp dict is not used until next cycle
        self._matched_lines_dict = None

    def finish_processing_all(self):
        self.print_objects()

        # TODO yarndevtoolsv2: Refactor to separate classes: latest failures, changed failures comparison, crosscheck with known failures
        self._aggregation_results._aggregated = AggregatedTestFailures(
            self._testcase_filters.get_aggregate_filters(),
            # TODO yarndevtoolsv2
            self._aggregation_results._test_failures_by_tcf,
        )
        self._aggregation_results._latest_failures = LatestTestFailures(
            self._testcase_filters.LATEST_FAILURE_FILTERS,
            # TODO yarndevtoolsv2
            self._aggregation_results._test_failures_by_tcf,
            only_last_results=True,
        )
        self._aggregation_results._comparison = TestFailureComparison(
            self._testcase_filters.LATEST_FAILURE_FILTERS,
            # TODO yarndevtoolsv2
            self._aggregation_results._test_failures_by_tcf,
            compare_with_last=True,
        )
        self._aggregation_results._known_failure_checker = KnownTestFailureChecker(
            self._testcase_filters.TESTCASES_TO_JIRAS_FILTERS, self._aggregation_results._aggregated
        )

    def get_failed_testcases_by_filter(self, tcf: TestCaseFilter) -> List[FailedTestCaseAbs]:
        return self._aggregation_results.get(tcf)

    def get_latest_failed_testcases_by_filter(self, tcf: TestCaseFilter) -> List[FailedTestCaseAbs]:
        return self._aggregation_results.get_latest_testcases(tcf)

    def get_build_comparison_result_by_filter(self, tcf: TestCaseFilter) -> BuildComparisonResult:
        return self._aggregation_results.get_build_comparison_results(tcf)

    def get_aggregated_testcases_by_filter(
        self, tcf: TestCaseFilter, filter_unknown=False, filter_reoccurred=False
    ) -> List[FailedTestCaseAggregated]:
        local_vars = locals()
        applied_filters = [name for name in local_vars if name.startswith("filter_") and local_vars[name]]
        filtered_tcs = self._aggregation_results.get_aggregated_testcases(tcf)
        original_length = len(filtered_tcs)
        prev_length = original_length
        if filter_unknown:
            filtered_tcs = list(filter(lambda tc: not tc.known_failure, filtered_tcs))
            LOG.debug(
                f"Filtering for unknown TCs. "
                f"Previous length of aggregated TCs: {prev_length}, "
                f"New length of filtered aggregated TCs: {len(filtered_tcs)}"
            )
            prev_length = len(filtered_tcs)
        if filter_reoccurred:
            filtered_tcs = list(filter(lambda tc: tc.reoccurred, filtered_tcs))
            LOG.debug(
                f"Filtering for reoccurred TCs. "
                f"Previous length of aggregated TCs: {prev_length}, "
                f"New length of filtered aggregated TCs: {len(filtered_tcs)}"
            )
            prev_length = len(filtered_tcs)

        LOG.debug(
            "Returning filtered aggregated TCs. "
            f"Original length of ALL aggregated TCs: {original_length}, "
            f"Length of filtered aggregated TCs: {prev_length}, "
            f"Applied filters: {applied_filters}"
        )
        return filtered_tcs

    def print_objects(self):
        pass
        # TODO should be trace logged
        # LOG.debug(f"All failed testcase objects: {self._failed_testcases}")


class UnitTestResultAggregatorEmailParserUtils:
    @staticmethod
    def create_parser(subparsers, command_type: CommandType, func_to_execute: Callable, add_gsheet_args=True):
        parser = subparsers.add_parser(
            command_type.name,
            help="Aggregates unit test results from a gmail account."
            "Example: "
            "--gsheet "
            "--gsheet-client-secret /Users/snemeth/.secret/dummy.json "
            "--gsheet-spreadsheet 'Failed testcases parsed from emails [generated by script]' "
            "--gsheet-worksheet 'Failed testcases'",
        )
        if add_gsheet_args:
            gsheet_group = GSheetArguments.add_gsheet_arguments(parser)

            gsheet_group.add_argument(
                "--gsheet-compare-with-jira-table",
                dest="gsheet_compare_with_jira_table",
                type=str,
                help="This should be provided if comparison of failed testcases with reported jira table must be performed. "
                "The value is a name to a worksheet, for example 'testcases with jiras'.",
            )

        parser.add_argument(
            "--account-email",
            required=True,
            type=str,
            help="Email address of Gmail account that will be used to Gmail API authentication and fetching data.",
        )

        parser.add_argument(
            "-q",
            "--gmail-query",
            required=True,
            type=str,
            help="Gmail query string that will be used to get emails to parse.",
        )

        parser.add_argument(
            "--smart-subject-query",
            action="store_true",
            default=False,
            help="Whether to fix Gmail queries like: 'Subject: YARN Daily unit test report', "
            "where the subject should have been between quotes.",
        )

        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            dest="verbose",
            default=None,
            required=False,
            help="More verbose log",
        )

        parser.add_argument(
            "-m",
            "--match-expression",
            required=False,
            # TODO
            type=ArgumentParserUtils.matches_match_expression_pattern,
            nargs="+",
            help="Line matcher expression, this will be converted to a regex. "
            "For example, if expression is org.apache, the regex will be .*org\\.apache\\.* "
            "Only lines in the mail content matching for this expression will be considered as a valid line.",
        )

        parser.add_argument(
            "-s",
            "--skip-lines-starting-with",
            required=False,
            type=str,
            nargs="+",
            help="If lines starting with these strings, they will not be considered as a line to parse",
        )

        parser.add_argument(
            "-l",
            "--request-limit",
            dest="request_limit",
            type=int,
            help="Limit the number of API requests",
        )

        parser.add_argument("--email-content-line-separator", type=str, default=DEFAULT_LINE_SEP)

        parser.add_argument(
            "--truncate-subject",
            dest="truncate_subject",
            type=str,
            help="Whether to truncate subject in outputs. The specified string will be cropped "
            "from the full value of subject strings when printing them to any destination.",
        )

        parser.add_argument(
            "--abbreviate-testcase-package",
            dest="abbrev_testcase_package",
            type=str,
            help="Whether to abbreviate testcase package names in outputs in order to save screen space. "
            "The specified string will be abbreviated with the starting letters."
            "For example, specifying 'org.apache.hadoop.yarn' will be converted to 'o.a.h.y' "
            "when printing testcase names to any destination.",
        )

        parser.add_argument(
            "--summary-mode",
            dest="summary_mode",
            type=str,
            choices=[sm.value for sm in SummaryMode],
            default=SummaryMode.HTML.value,
            help="Summary file(s) will be written in this mode. Defaults to HTML.",
        )

        parser.add_argument(
            "--aggregate-filters",
            dest="aggregate_filters",
            required=True,
            type=str,
            nargs="+",
            help="Execute some post filters on the email results. "
            "The resulted emails and testcases for each filter will be aggregated to "
            "a separate worksheet with name <WS>_aggregated_<aggregate-filter> where WS is equal to the "
            "value specified by the --gsheet-worksheet argument.",
        )

        exclusive_group = parser.add_mutually_exclusive_group(required=True)
        exclusive_group.add_argument(
            "-p", "--print", action="store_true", dest="do_print", help="Print results to console", required=False
        )
        exclusive_group.add_argument(
            "-g",
            "--gsheet",
            action="store_true",
            dest="gsheet",
            default=False,
            required=False,
            help="Export values to Google sheet. Additional gsheet arguments need to be specified!",
        )

        parser.set_defaults(func=func_to_execute)


class EmailBasedUnitTestResultAggregatorConfig:
    # TODO yarndevtoolsv2: Revisit any common logic / config for email+db based aggregator?
    def __init__(self, parser, args, output_dir: str):
        self._validate_args(parser, args)
        self.console_mode = getattr(args, "console mode", False)
        self.gmail_query = args.gmail_query
        self.smart_subject_query = args.smart_subject_query
        self.request_limit = getattr(args, "request_limit", 1000000)
        self.account_email: str = args.account_email
        self.testcase_filters = TestCaseFilters(
            TestCaseFilters.convert_raw_match_expressions_to_objs(getattr(args, "match_expression", None)),
            self._get_attribute(args, "aggregate_filters", default=[]),
        )
        self.skip_lines_starting_with: List[str] = getattr(args, "skip_lines_starting_with", [])
        self.email_content_line_sep = getattr(args, "email_content_line_separator", DEFAULT_LINE_SEP)
        self.truncate_subject_with: str = getattr(args, "truncate_subject", None)
        self.abbrev_tc_package: str = getattr(args, "abbrev_testcase_package", None)
        self.summary_mode = args.summary_mode
        self.output_dir = output_dir
        self.email_cache_dir = FileUtils.join_path(output_dir, "email_cache")
        self.session_dir = ProjectUtils.get_session_dir_under_child_dir(FileUtils.basename(output_dir))
        self.full_cmd: str = OsUtils.determine_full_command_filtered(filter_password=True)

        if self.operation_mode == OperationMode.GSHEET:
            worksheet_names: List[str] = [
                self.get_worksheet_name(tcf) for tcf in self.testcase_filters.ALL_VALID_FILTERS
            ]
            LOG.info(
                f"Adding worksheets to {self.gsheet_options.__class__.__name__}. "
                f"Generated worksheet names: {worksheet_names}"
            )
            for worksheet_name in worksheet_names:
                self.gsheet_options.add_worksheet(worksheet_name)

    @staticmethod
    def _get_attribute(args, attr_name, default=None):
        val = getattr(args, attr_name)
        if not val:
            return default
        return val

    def _validate_args(self, parser, args):
        if args.gsheet and (
            args.gsheet_client_secret is None or args.gsheet_spreadsheet is None or args.gsheet_worksheet is None
        ):
            parser.error(
                "--gsheet requires the following arguments: "
                "--gsheet-client-secret, --gsheet-spreadsheet and --gsheet-worksheet."
            )

        if args.do_print:
            self.operation_mode = OperationMode.PRINT
        elif args.gsheet:
            self.operation_mode = OperationMode.GSHEET
            self.gsheet_options = GSheetOptions(args.gsheet_client_secret, args.gsheet_spreadsheet, worksheet=None)
            self.gsheet_jira_table = getattr(args, "gsheet_compare_with_jira_table", None)
        if self.operation_mode not in VALID_OPERATION_MODES:
            raise ValueError(
                f"Unknown state! "
                f"Operation mode should be any of {VALID_OPERATION_MODES}, but it is set to: {self.operation_mode}"
            )
        if hasattr(args, "gmail_credentials_file"):
            FileUtils.ensure_file_exists(args.gmail_credentials_file)

    def __str__(self):
        return (
            f"Full command was: {self.full_cmd}\n"
            f"Output dir: {self.output_dir}\n"
            f"Account email: {self.account_email}\n"
            f"Email cache dir: {self.email_cache_dir}\n"
            f"Session dir: {self.session_dir}\n"
            f"Console mode: {self.console_mode}\n"
            f"Gmail query: {self.gmail_query}\n"
            f"Smart subject query: {self.smart_subject_query}\n"
            f"Testcase filters: {self.testcase_filters}\n"
            f"Email line separator: {self.email_content_line_sep}\n"
            f"Request limit: {self.request_limit}\n"
            f"Operation mode: {self.operation_mode}\n"
            f"Skip lines starting with: {self.skip_lines_starting_with}\n"
            f"Truncate subject with: {self.truncate_subject_with}\n"
            f"Abbreviate testcase package: {self.abbrev_tc_package}\n"
            f"Summary mode: {self.summary_mode}\n"
        )

    @staticmethod
    def get_worksheet_name(tcf: TestCaseFilter):
        ws_name: str = f"{tcf.match_expr.alias}"
        if tcf.aggr_filter:
            ws_name += f"_{tcf.aggr_filter.val}_{AGGREGATED_WS_POSTFIX}"
        elif tcf.aggregate:
            ws_name += f"_{AGGREGATED_WS_POSTFIX}"
        else:
            ws_name += f"_{MATCHTYPE_ALL_POSTFIX}"
        return f"{ws_name}"


class EmailUtilsForAggregators:
    def __init__(self, config, command_type):
        self.config = config
        self.command_type = command_type
        self.gmail_wrapper = None

    def init_gmail(self):
        self.gmail_wrapper = self.setup_gmail_wrapper()

    def setup_gmail_wrapper(self):
        google_auth = GoogleApiAuthorizer(
            ServiceType.GMAIL,
            project_name=f"{self.command_type.output_dir_name}",
            secret_basedir=SECRET_PROJECTS_DIR,
            account_email=self.config.account_email,
        )
        return GmailWrapper(google_auth, output_basedir=self.config.email_cache_dir)

    def fetch_known_test_failures(self):
        if self.config.operation_mode == OperationMode.GSHEET:
            gsheet_wrapper = GSheetWrapper(self.config.gsheet_options)
            return KnownTestFailures(gsheet_wrapper=gsheet_wrapper, gsheet_jira_table=self.config.gsheet_jira_table)
        return None

    def get_gmail_query(self):
        original_query = self.config.gmail_query
        if self.config.smart_subject_query and original_query.startswith(SUBJECT):
            real_subject = original_query.split(SUBJECT)[1]
            logical_expressions = [" and ", " or "]
            if any(x in real_subject.lower() for x in logical_expressions):
                LOG.warning(f"Detected logical expression in query, won't modify original query: {original_query}")
                return original_query
            if " " in real_subject and real_subject[0] != '"':
                fixed_subject = f'"{real_subject}"'
                new_query = SUBJECT + fixed_subject
                LOG.info(
                    f"Fixed Gmail query string.\n"
                    f"Original query string: {original_query}\n"
                    f"New query string: {new_query}"
                )
                return new_query
        return original_query

    def perform_gmail_query(self):
        query_result: ThreadQueryResults = self.gmail_wrapper.query_threads(
            query=self.get_gmail_query(), limit=self.config.request_limit, expect_one_message_per_thread=True
        )
        LOG.info(
            f"Received thread query result:\n"
            f"Number of threads: {query_result.no_of_threads}\n"
            f"Number of messages: {query_result.no_of_messages}\n"
            f"Number of unique subjects: {len(query_result.unique_subjects)}\n"
            f"Unique subjects: {pformat(query_result.unique_subjects)}"
        )
        return query_result

    @staticmethod
    def check_if_line_is_valid(line, skip_lines_starting_with):
        valid_line = True
        for skip_str in skip_lines_starting_with:
            if line.startswith(skip_str):
                valid_line = False
                break
        return valid_line

    @staticmethod
    def process_gmail_results(
        query_result: ThreadQueryResults,
        result: EmailBasedAggregationResults,
        split_body_by: str,
        skip_lines_starting_with: List[str],
    ):
        for message in query_result.threads.messages:
            LOG.debug("Processing message: %s", message.subject)
            msg_parts = message.get_all_plain_text_parts()
            for msg_part in msg_parts:
                lines = msg_part.body_data.split(split_body_by)
                result.start_new_context()
                for line in lines:
                    line = line.strip()
                    # TODO this compiles the pattern over and over again --> Create a new helper function that receives a compiled pattern
                    if not EmailUtilsForAggregators.check_if_line_is_valid(line, skip_lines_starting_with):
                        LOG.warning(f"Skipping invalid line: {line} [Mail subject: {message.subject}]")
                        continue
                    result.match_line(line, message.subject)
                result.finish_context(message)
        result.finish_processing_all()


@dataclass
class EmailMetaData:
    message_id: str
    thread_id: str
    subject: str
    date: datetime.datetime


@auto_str
class FailedTestCaseFromEmail(FailedTestCase):
    def __init__(self, full_name, email_meta: EmailMetaData):
        super().__init__(full_name)
        self.email_meta: EmailMetaData = email_meta

    def date(self) -> datetime.datetime:
        return self.email_meta.date

    def subject(self):
        return self.email_meta.subject
