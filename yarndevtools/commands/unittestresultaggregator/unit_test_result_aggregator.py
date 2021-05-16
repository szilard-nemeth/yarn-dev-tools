import datetime
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Set

from googleapiwrapper.common import ServiceType
from googleapiwrapper.gmail_api import GmailWrapper, ThreadQueryResults
from googleapiwrapper.gmail_domain import GmailMessage
from googleapiwrapper.google_auth import GoogleApiAuthorizer
from googleapiwrapper.google_sheet import GSheetOptions, GSheetWrapper
from pythoncommons.date_utils import DateUtils
from pythoncommons.file_utils import FileUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils
from pythoncommons.string_utils import RegexUtils

from yarndevtools.commands.unittestresultaggregator.common import (
    MATCH_ALL_LINES_EXPRESSION,
    MATCHTYPE_ALL_POSTFIX,
    get_key_by_testcase_filter,
    MatchExpression,
    OperationMode,
    TestCaseFilter,
    FailedTestCase,
    EmailMetaData,
    KnownTestFailureInJira,
    MATCH_EXPRESSION_SEPARATOR,
    REGEX_EVERYTHING,
    AggregateFilter,
)
from yarndevtools.commands.unittestresultaggregator.representation import SummaryGenerator, UnitTestResultOutputManager
from yarndevtools.common.shared_command_utils import SECRET_PROJECTS_DIR
from yarndevtools.constants import UNIT_TEST_RESULT_AGGREGATOR

VALID_OPERATION_MODES = [OperationMode.PRINT, OperationMode.GSHEET]

LOG = logging.getLogger(__name__)

AGGREGATED_WS_POSTFIX = "aggregated"
SUBJECT = "subject:"
DEFAULT_LINE_SEP = "\\r\\n"


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
            getattr(args, "aggregate_filters", []),
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
            worksheet_names: List[str] = []
            tc_filters = self.testcase_filters.get_testcase_filter_objs(
                extended_expressions=True, match_expr_separately_always=True
            )
            for tcf in tc_filters:
                worksheet_name = self.get_worksheet_name(tcf)
                worksheet_names.append(worksheet_name)
                self.gsheet_options.add_worksheet(worksheet_name)
            LOG.info(f"Generated worksheet names: {worksheet_names}")

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
        else:
            ws_name += f"_{MATCHTYPE_ALL_POSTFIX}"
        return f"{ws_name}"


@dataclass
class TestCaseFilters:
    match_expressions: List[MatchExpression]
    aggregate_filters: List[AggregateFilter]

    def __post_init__(self):
        if not all([isinstance(af, str) or isinstance(af, AggregateFilter) for af in self.aggregate_filters]):
            raise ValueError(f"Mixed instances in self.aggregate_filters: {self.aggregate_filters}")

        tmp_list: List[AggregateFilter] = []
        for aggr_filter in self.aggregate_filters:
            if isinstance(aggr_filter, str):
                tmp_list.append(AggregateFilter(aggr_filter))

        if tmp_list:
            self.aggregate_filters = tmp_list
        self.AGGREGATION_FILTERS: List[TestCaseFilter] = self.get_testcase_filter_objs(
            extended_expressions=False, match_expr_if_no_aggr_filter=True
        )
        self.LATEST_FAILURE_FILTERS = self.get_testcase_filter_objs(
            match_expr_separately_always=False, match_expr_if_no_aggr_filter=False, without_aggregates=False
        )

    @property
    def extended_match_expressions(self) -> List[MatchExpression]:
        return self.match_expressions + [MATCH_ALL_LINES_EXPRESSION]

    def get_testcase_filter_objs(
        self,
        extended_expressions=False,
        match_expr_separately_always=False,
        match_expr_if_no_aggr_filter=False,
        without_aggregates=False,
    ) -> List[TestCaseFilter]:
        match_expressions_list = self.extended_match_expressions if extended_expressions else self.match_expressions

        result: List[TestCaseFilter] = []
        for match_expr in match_expressions_list:
            if match_expr_separately_always:
                result.append(TestCaseFilter(match_expr, None))
            elif match_expr_if_no_aggr_filter and not self.aggregate_filters:
                result.append(TestCaseFilter(match_expr, None))

            if without_aggregates:
                continue

            # We don't need aggregate for all lines
            if match_expr != MATCH_ALL_LINES_EXPRESSION:
                for aggr_filter in self.aggregate_filters:
                    result.append(TestCaseFilter(match_expr, aggr_filter))
        return result

    def match_all_lines(self) -> bool:
        return len(self.match_expressions) == 1 and self.match_expressions[0] == MATCH_ALL_LINES_EXPRESSION

    @staticmethod
    def convert_raw_match_expressions_to_objs(raw_match_exprs: List[str]) -> List[MatchExpression]:
        if not raw_match_exprs:
            return [MATCH_ALL_LINES_EXPRESSION]

        match_expressions: List[MatchExpression] = []
        for raw_match_expr in raw_match_exprs:
            segments = raw_match_expr.split(MATCH_EXPRESSION_SEPARATOR)
            alias = segments[0]
            if alias == MATCHTYPE_ALL_POSTFIX:
                raise ValueError(
                    f"Alias for match expression '{MATCHTYPE_ALL_POSTFIX}' is reserved. Please use another alias."
                )
            match_expr = segments[1]
            pattern = REGEX_EVERYTHING + match_expr.replace(".", "\\.") + REGEX_EVERYTHING
            match_expressions.append(MatchExpression(alias, raw_match_expr, pattern))
        return match_expressions


@dataclass(eq=True, frozen=True)
class TestCaseKey:
    tc_filter: TestCaseFilter
    full_name: str
    email_subject: str

    @staticmethod
    def create_from(tcf: TestCaseFilter, ftc: FailedTestCase, use_full_name=True, use_simple_name=False):
        if all([use_full_name, use_simple_name]) or not any([use_full_name, use_simple_name]):
            raise ValueError("Either 'use_simple_name' or 'use_full_name' should be set to True, but not both!")
        tc_name = ftc.full_name if use_full_name else None
        tc_name = ftc.simple_name if use_simple_name else tc_name
        return TestCaseKey(tcf, tc_name, ftc.email_meta.subject)


@dataclass
class FailedTestCases:
    _failed_tcs: Dict[TestCaseFilter, List[FailedTestCase]] = field(default_factory=dict)
    _aggregation_completed: Set[TestCaseFilter] = field(default_factory=set)

    def __post_init__(self):
        self._tc_keys: Dict[TestCaseKey, FailedTestCase] = {}
        self._latest_testcases: Dict[TestCaseFilter, List[FailedTestCase]] = defaultdict(list)

    def _init_if_required(self, tcf: TestCaseFilter):
        if tcf not in self._failed_tcs:
            self._failed_tcs[tcf] = []

    def _add_known_failed_testcase(self, tc_key: TestCaseKey, ftc: FailedTestCase):
        self._tc_keys[tc_key] = ftc

    def add_failure(self, tcf: TestCaseFilter, failed_testcase: FailedTestCase):
        if tcf not in self._failed_tcs:
            self._failed_tcs[tcf] = []
        tc_key = TestCaseKey.create_from(tcf, failed_testcase)
        if tc_key in self._tc_keys:
            stored_testcase = self._tc_keys[tc_key]
            LOG.debug(
                f"Found already existing testcase key: {tc_key}. "
                f"Value: {stored_testcase}, "
                f"Email data (stored): {stored_testcase.email_meta.subject} "
                f"Email data (new): {stored_testcase.email_meta.subject}"
            )
            return
        else:
            self._add_known_failed_testcase(tc_key, failed_testcase)

        self._failed_tcs[tcf].append(failed_testcase)

    def get(self, tcf) -> List[FailedTestCase]:
        return self._failed_tcs[tcf]

    def get_latest_testcases(self, tcf) -> List[FailedTestCase]:
        return self._latest_testcases[tcf]

    def print_keys(self):
        LOG.debug(f"Keys of _failed_testcases_by_filter: {self._failed_tcs.keys()}")

    def aggregate(self, testcase_filters: List[TestCaseFilter]):
        for tcf in testcase_filters:
            failure_freq: Dict[TestCaseKey, int] = {}
            latest_failure: Dict[TestCaseKey, datetime.datetime] = {}
            tc_key_to_testcases: Dict[TestCaseKey, List[FailedTestCase]] = defaultdict(list)
            for testcase in self._failed_tcs[tcf]:
                tc_key = TestCaseKey.create_from(tcf, testcase, use_simple_name=True, use_full_name=False)
                tc_key_to_testcases[tc_key].append(testcase)
                if tc_key not in failure_freq:
                    failure_freq[tc_key] = 1
                    latest_failure[tc_key] = testcase.email_meta.date
                else:
                    LOG.debug(
                        "Found TC key in failure_freq dict. "
                        f"Current TC: {testcase}, "
                        f"Previously stored TC: {failure_freq[tc_key]}, "
                    )
                    failure_freq[tc_key] = failure_freq[tc_key] + 1
                    if testcase.email_meta.date > latest_failure[tc_key]:
                        latest_failure[tc_key] = testcase.email_meta.date
            self._aggregation_completed.add(tcf)

            for tc_key, testcases in tc_key_to_testcases.items():
                for tc in testcases:
                    tc.latest_failure = latest_failure[tc_key]
                    tc.failure_freq = failure_freq[tc_key]

    def create_latest_failures(
        self,
        testcase_filters: List[TestCaseFilter],
        last_n_days=None,
        only_last_results=False,
        reset_oldest_day_to_midnight=False,
    ):
        if sum([True if last_n_days else False, only_last_results]) != 1:
            raise ValueError("Either last_n_days or only_last_results mode should be enabled.")

        for tcf in testcase_filters:
            failed_testcases = self._failed_tcs[tcf]
            sorted_testcases = sorted(failed_testcases, key=lambda ftc: ftc.email_meta.date, reverse=True)
            if not sorted_testcases:
                return []

            if last_n_days:
                date_range_open = self._get_date_range_open(last_n_days, reset_oldest_day_to_midnight)
                LOG.info(f"Using date range open date to filter dates: {date_range_open}")
            else:
                date_range_open = sorted_testcases[0].email_meta.date

            for testcase in sorted_testcases:
                if testcase.email_meta.date >= date_range_open:
                    self._latest_testcases[tcf].append(testcase)

    @staticmethod
    def _get_date_range_open(last_n_days, reset_oldest_day_to_midnight):
        oldest_day: datetime.datetime = DateUtils.get_current_time_minus(days=last_n_days)
        if reset_oldest_day_to_midnight:
            oldest_day = DateUtils.reset_to_midnight(oldest_day)
        return oldest_day


class TestcaseFilterResults:
    def __init__(self, testcase_filters: TestCaseFilters):
        self.testcase_filters: TestCaseFilters = testcase_filters
        self.match_all_lines: bool = self._should_match_all_lines()
        self._failed_testcases: FailedTestCases = FailedTestCases()

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
        # Prepare matched_lines dict with all required empty-lists for match expressions and aggregate filters
        self._matched_lines_dict: Dict[str, List[str]] or None = {}
        self._add_matched_lines([], TestCaseFilter(MATCH_ALL_LINES_EXPRESSION, None))
        filters: List[TestCaseFilter] = self.testcase_filters.get_testcase_filter_objs(
            match_expr_separately_always=True
        )
        for tcf in filters:
            self._add_matched_lines([], tcf)

    def _add_matched_lines(self, lines: List[str], tcf: TestCaseFilter):
        self._matched_lines_dict[self._get_matched_lines_key(tcf)] = lines

    def match_line(self, line, mail_subject: str):
        matches_any_pattern, matched_expression = self._does_line_match_any_match_expression(line, mail_subject)
        if self.match_all_lines or matches_any_pattern:
            self._matched_lines_dict[MATCHTYPE_ALL_POSTFIX].append(line)
            tcf = TestCaseFilter(matched_expression, None)
            self._matched_lines_dict[self._get_matched_lines_key(tcf)].append(line)

            for aggr_filter in self.testcase_filters.aggregate_filters:
                if aggr_filter.val in mail_subject:
                    LOG.debug(
                        f"Found matching email subject for aggregation filter '{aggr_filter}': "
                        f"Subject: {mail_subject}"
                    )
                    tcf = TestCaseFilter(matched_expression, aggr_filter)
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
            self._str_key_to_testcase_filter[MATCHTYPE_ALL_POSTFIX] = TestCaseFilter(MATCH_ALL_LINES_EXPRESSION, None)
            return MATCHTYPE_ALL_POSTFIX
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
        self._failed_testcases.aggregate(self.testcase_filters.AGGREGATION_FILTERS)
        self._failed_testcases.create_latest_failures(
            self.testcase_filters.LATEST_FAILURE_FILTERS, only_last_results=True
        )

    def get_failed_testcases_by_filter(self, tcf: TestCaseFilter) -> List[FailedTestCase]:
        return self._failed_testcases.get(tcf)

    def get_latest_failed_testcases_by_filter(self, tcf: TestCaseFilter) -> List[FailedTestCase]:
        return self._failed_testcases.get_latest_testcases(tcf)

    def print_objects(self):
        LOG.debug(f"All failed testcase objects: {self._failed_testcases}")


class UnitTestResultAggregator:
    def __init__(self, args, parser, output_dir: str):
        self.config = UnitTestResultAggregatorConfig(parser, args, output_dir)
        if self.config.operation_mode == OperationMode.GSHEET:
            self.gsheet_wrapper: GSheetWrapper or None = GSheetWrapper(self.config.gsheet_options)
            self.testcases_to_jiras = []
            if self.config.gsheet_jira_table:
                self._load_and_convert_known_test_failures_in_jira()
        else:
            # Avoid AttributeError
            self.gsheet_wrapper = None
        self.authorizer = GoogleApiAuthorizer(
            ServiceType.GMAIL,
            project_name=f"{UNIT_TEST_RESULT_AGGREGATOR}",
            secret_basedir=SECRET_PROJECTS_DIR,
            account_email=self.config.account_email,
        )
        self.gmail_wrapper = GmailWrapper(self.authorizer, output_basedir=self.config.email_cache_dir)

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
        LOG.info(f"Received thread query result: {query_result}")
        tc_filter_results: TestcaseFilterResults = self.filter_query_result_data(query_result)

        output_manager = UnitTestResultOutputManager(
            self.config.session_dir, self.config.console_mode, self.gsheet_wrapper
        )
        SummaryGenerator.process_testcase_filter_results(
            tc_filter_results, query_result, self.config, output_manager, self.testcases_to_jiras
        )

    def filter_query_result_data(self, query_result: ThreadQueryResults) -> TestcaseFilterResults:
        tc_filter_results = TestcaseFilterResults(self.config.testcase_filters)
        for message in query_result.threads.messages:
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
