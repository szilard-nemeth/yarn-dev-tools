import logging
from collections import defaultdict
from typing import List, Dict, Tuple

from googleapiwrapper.common import ServiceType
from googleapiwrapper.gmail_api import ThreadQueryResults, GmailWrapper
from googleapiwrapper.gmail_domain import GmailMessage
from googleapiwrapper.google_auth import GoogleApiAuthorizer
from googleapiwrapper.google_sheet import GSheetOptions, GSheetWrapper
from pythoncommons.date_utils import DateUtils
from pythoncommons.file_utils import FileUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils
from pythoncommons.string_utils import RegexUtils
from pprint import pformat

from yarndevtools.cdsw.constants import SECRET_PROJECTS_DIR
from yarndevtools.commands.unittestresultaggregator.common import (
    OperationMode,
    VALID_OPERATION_MODES,
    MATCHTYPE_ALL_POSTFIX,
    AGGREGATED_WS_POSTFIX,
    TestCaseFilter,
    TestCaseFilters,
    KnownTestFailureInJira,
    SummaryMode,
    FailedTestCaseAggregated,
    get_key_by_testcase_filter,
    EmailMetaData,
    FailedTestCase,
    MATCH_ALL_LINES_EXPRESSION,
    MatchExpression,
    FailedTestCases,
    BuildComparisonResult,
)
from yarndevtools.commands.unittestresultaggregator.representation import UnitTestResultOutputManager, SummaryGenerator
from yarndevtools.commands_common import CommandAbs, GSheetArguments, ArgumentParserUtils
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.yarn_dev_tools_config import YarnDevToolsConfig

LOG = logging.getLogger(__name__)
SUBJECT = "subject:"
DEFAULT_LINE_SEP = "\\r\\n"


# TODO yarndevtoolsv2: consider extracting common aggregation logic from this class / or create abstraction layer?
class TestcaseFilterResults:
    def __init__(self, testcase_filters: TestCaseFilters, testcases_to_jiras: List[KnownTestFailureInJira]):
        self.testcases_to_jiras = testcases_to_jiras
        self.testcase_filters: TestCaseFilters = testcase_filters
        self.match_all_lines: bool = self._should_match_all_lines()
        self._failed_testcases: FailedTestCases = FailedTestCases()

        all_filters: List[TestCaseFilter] = self.testcase_filters.ALL_VALID_FILTERS
        self._failed_testcases.init_with_testcase_filters(all_filters)

        # This is a temporary dict - usually for a context of a message
        self._matched_lines_dict: Dict[str, List[str]] = {}
        self._str_key_to_testcase_filter: Dict[str, TestCaseFilter] = {}

    def _should_match_all_lines(self):
        match_all_lines: bool = self.testcase_filters.match_all_lines()
        LOG.info(
            "**Matching all lines"
            if match_all_lines
            else f"**Matching lines with regex pattern: {self.testcase_filters.match_expressions}"
        )
        return match_all_lines

    def start_new_context(self):
        # Prepare matched_lines dict with all required empty-lists for ALL filters
        self._matched_lines_dict = defaultdict(list)
        all_filters: List[TestCaseFilter] = self.testcase_filters.ALL_VALID_FILTERS

        # Do sanity check
        generated_keys = [self._get_matched_lines_key(tcf) for tcf in all_filters]
        unique_keys = set(generated_keys)
        if len(all_filters) != len(unique_keys):
            raise ValueError(
                "Mismatch in number of testcase filter objects and generated keys. "
                f"Filters: {all_filters}, "
                f"Generated keys: {generated_keys}, "
                f"Unique keys: {unique_keys}."
            )

        for tcf in all_filters:
            self._matched_lines_dict[self._get_matched_lines_key(tcf)] = []

    def match_line(self, line, mail_subject: str):
        matches_any_pattern, matched_expression = self._does_line_match_any_match_expression(line, mail_subject)
        if self.match_all_lines or matches_any_pattern:
            self._matched_lines_dict[MATCHTYPE_ALL_POSTFIX].append(line)
            self._add_match_to_matched_lines_dict(line, matched_expression, aggregate_values=[True, False])

            for aggr_filter in self.testcase_filters.aggregate_filters:
                if aggr_filter.val in mail_subject:
                    LOG.debug(
                        f"Found matching email subject for aggregation filter '{aggr_filter}': "
                        f"Subject: {mail_subject}"
                    )
                    tcf = TestCaseFilter(matched_expression, aggr_filter)
                    self._matched_lines_dict[self._get_matched_lines_key(tcf)].append(line)

    def _add_match_to_matched_lines_dict(self, line, matched_expression, aggregate_values: List[bool]):
        for aggr_value in aggregate_values:
            tcf = TestCaseFilter(matched_expression, None, aggregate=aggr_value)
            self._matched_lines_dict[self._get_matched_lines_key(tcf)].append(line)

    def _does_line_match_any_match_expression(self, line, mail_subject: str) -> Tuple[bool, MatchExpression or None]:
        for match_expression in self.testcase_filters.match_expressions:
            if RegexUtils.ensure_matches_pattern(line, match_expression.pattern):
                LOG.debug(f"Matched line: {line} [Mail subject: {mail_subject}]")
                return True, match_expression
        LOG.debug(f"Line did not match for any pattern: {line}")
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
        LOG.debug(f"Keys of _matched_lines_dict: {self._matched_lines_dict.keys()}")
        for key, matched_lines in self._matched_lines_dict.items():
            if not matched_lines:
                continue
            tcf: TestCaseFilter = self._str_key_to_testcase_filter[key]
            for matched_line in matched_lines:
                email_meta = EmailMetaData(message.msg_id, message.thread_id, message.subject, message.date)
                failed_testcase = FailedTestCase(matched_line, email_meta)
                self._failed_testcases.add_failure(tcf, failed_testcase)

        self._failed_testcases.print_keys()
        # Make sure temp dict is not used until next cycle
        self._matched_lines_dict = None

    def finish_processing_all(self):
        for tcf in self.testcase_filters.ALL_VALID_FILTERS:
            self._failed_testcases.init_comparison_results(tcf)

        self._failed_testcases.aggregate(self.testcase_filters.get_aggregate_filters())
        self._failed_testcases.create_latest_failures(
            self.testcase_filters.LATEST_FAILURE_FILTERS, only_last_results=True
        )
        self._failed_testcases.create_changed_failures_comparison(
            self.testcase_filters.LATEST_FAILURE_FILTERS, compare_with_last=True
        )
        self._failed_testcases.cross_check_testcases_with_jiras(
            self.testcase_filters.TESTCASES_TO_JIRAS_FILTERS, self.testcases_to_jiras
        )

    def get_failed_testcases_by_filter(self, tcf: TestCaseFilter) -> List[FailedTestCase]:
        return self._failed_testcases.get(tcf)

    def get_latest_failed_testcases_by_filter(self, tcf: TestCaseFilter) -> List[FailedTestCase]:
        return self._failed_testcases.get_latest_testcases(tcf)

    def get_build_comparison_result_by_filter(self, tcf: TestCaseFilter) -> BuildComparisonResult:
        return self._failed_testcases.get_build_comparison_results(tcf)

    def get_aggregated_testcases_by_filter(
        self, tcf: TestCaseFilter, filter_unknown=False, filter_reoccurred=False
    ) -> List[FailedTestCaseAggregated]:
        local_vars = locals()
        applied_filters = [name for name in local_vars if name.startswith("filter_") and local_vars[name]]
        filtered_tcs = self._failed_testcases.get_aggregated_testcases(tcf)
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
        LOG.debug(f"All failed testcase objects: {self._failed_testcases}")


# TODO yarndevtoolsv2: Revisit any common logic / config for email+db based aggregator?
class UnitTestResultAggregatorConfig:
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


# TODO yarndevtoolsv2: Revisit any common logic for email+db based aggregator?
class UnitTestResultAggregator(CommandAbs):
    def __init__(self, args, parser, output_dir: str):
        self.config = UnitTestResultAggregatorConfig(parser, args, output_dir)
        self.testcases_to_jiras = []
        if self.config.operation_mode == OperationMode.GSHEET:
            self.gsheet_wrapper: GSheetWrapper or None = GSheetWrapper(self.config.gsheet_options)
            self.testcases_to_jiras: List[KnownTestFailureInJira] = []
            if self.config.gsheet_jira_table:
                self._load_and_convert_known_test_failures_in_jira()
        else:
            # Avoid AttributeError
            self.gsheet_wrapper = None
        self.authorizer = GoogleApiAuthorizer(
            ServiceType.GMAIL,
            project_name=f"{CommandType.UNIT_TEST_RESULT_AGGREGATOR.output_dir_name}",
            secret_basedir=SECRET_PROJECTS_DIR,
            account_email=self.config.account_email,
        )
        self.gmail_wrapper = GmailWrapper(self.authorizer, output_basedir=self.config.email_cache_dir)

    @staticmethod
    def create_parser(subparsers):
        parser = subparsers.add_parser(
            CommandType.UNIT_TEST_RESULT_AGGREGATOR.name,
            help="Aggregates unit test results from a gmail account."
            "Example: "
            "--gsheet "
            "--gsheet-client-secret /Users/snemeth/.secret/dummy.json "
            "--gsheet-spreadsheet 'Failed testcases parsed from emails [generated by script]' "
            "--gsheet-worksheet 'Failed testcases'",
        )
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
            help="Whether to abbreviate testcase package names in outputs in order to screen space. "
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

        parser.set_defaults(func=UnitTestResultAggregator.execute)

    @staticmethod
    def execute(args, parser=None):
        output_dir = ProjectUtils.get_output_child_dir(CommandType.UNIT_TEST_RESULT_AGGREGATOR.output_dir_name)
        ut_results_aggregator = UnitTestResultAggregator(args, parser, output_dir)
        FileUtils.create_symlink_path_dir(
            CommandType.UNIT_TEST_RESULT_AGGREGATOR.session_link_name,
            ut_results_aggregator.config.session_dir,
            YarnDevToolsConfig.PROJECT_OUT_ROOT,
        )
        ut_results_aggregator.run()

    def _load_and_convert_known_test_failures_in_jira(self):
        raw_data_from_gsheet = self.gsheet_wrapper.read_data(self.config.gsheet_jira_table, "A1:E150")
        LOG.info(f"Successfully loaded data from worksheet: {self.config.gsheet_jira_table}")

        header: List[str] = raw_data_from_gsheet[0]
        expected_header = ["Testcase", "Jira", "Resolution date"]
        if header != expected_header:
            raise ValueError(
                "Detected suspicious known test failures table header. "
                f"Expected header: {expected_header}, "
                f"Current header: {header}"
            )

        raw_data_from_gsheet = raw_data_from_gsheet[1:]
        for r in raw_data_from_gsheet:
            row_len = len(r)
            if row_len < 2:
                raise ValueError(
                    "Both 'Testcase' and 'Jira' are mandatory items but row does not contain them. "
                    f"Problematic row: {r}"
                )
            # In case of 'Resolution date' is missing, append an empty-string so that all rows will have
            # an equal number of cells. This eases further processing.
            if row_len == 2:
                r.append("")
        self.testcases_to_jiras: List[KnownTestFailureInJira] = [
            KnownTestFailureInJira(r[0], r[1], DateUtils.convert_to_datetime(r[2], "%m/%d/%Y") if r[2] else None)
            for r in raw_data_from_gsheet
        ]

    def run(self):
        LOG.info(f"Starting Unit test result aggregator. Config: \n{str(self.config)}")
        gmail_query: str = self._get_gmail_query()
        query_result: ThreadQueryResults = self.gmail_wrapper.query_threads(
            query=gmail_query, limit=self.config.request_limit, expect_one_message_per_thread=True
        )
        LOG.info(
            f"Received thread query result:\n"
            f"Number of threads: {query_result.no_of_threads}\n"
            f"Number of messages: {query_result.no_of_messages}\n"
            f"Number of unique subjects: {len(query_result.unique_subjects)}\n"
            f"Unique subjects: {pformat(query_result.unique_subjects)}"
        )
        tc_filter_results: TestcaseFilterResults = self.filter_query_result_data(query_result, self.testcases_to_jiras)

        output_manager = UnitTestResultOutputManager(
            self.config.session_dir, self.config.console_mode, self.gsheet_wrapper
        )
        SummaryGenerator.process_testcase_filter_results(tc_filter_results, query_result, self.config, output_manager)

    def filter_query_result_data(
        self, query_result: ThreadQueryResults, testcases_to_jiras: List[KnownTestFailureInJira]
    ) -> TestcaseFilterResults:
        tc_filter_results = TestcaseFilterResults(self.config.testcase_filters, testcases_to_jiras)
        for message in query_result.threads.messages:
            LOG.debug("Processing message: %s", message.subject)
            msg_parts = message.get_all_plain_text_parts()
            for msg_part in msg_parts:
                lines = msg_part.body_data.split(self.config.email_content_line_sep)
                tc_filter_results.start_new_context()
                for line in lines:
                    line = line.strip()
                    # TODO this compiles the pattern over and over again --> Create a new helper function that receives a compiled pattern
                    if not self._check_if_line_is_valid(line, self.config.skip_lines_starting_with):
                        LOG.warning(f"Skipping invalid line: {line} [Mail subject: {message.subject}]")
                        continue
                    tc_filter_results.match_line(line, message.subject)
                tc_filter_results.finish_context(message)
        tc_filter_results.print_objects()
        tc_filter_results.finish_processing_all()
        return tc_filter_results

    @staticmethod
    def _check_if_line_is_valid(line, skip_lines_starting_with):
        valid_line = True
        for skip_str in skip_lines_starting_with:
            if line.startswith(skip_str):
                valid_line = False
                break
        return valid_line

    def _get_gmail_query(self):
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
                    f"Fixed gmail query string.\n"
                    f"Original query string: {original_query}\n"
                    f"New query string: {new_query}"
                )
                return new_query
        return original_query
