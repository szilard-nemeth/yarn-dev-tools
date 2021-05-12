import logging
from typing import List, Dict, Tuple

from googleapiwrapper.common import ServiceType
from googleapiwrapper.gmail_api import GmailWrapper, ThreadQueryResults
from googleapiwrapper.gmail_domain import GmailMessage
from googleapiwrapper.google_auth import GoogleApiAuthorizer
from googleapiwrapper.google_sheet import GSheetOptions, GSheetWrapper
from pythoncommons.file_utils import FileUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils
from pythoncommons.string_utils import RegexUtils

from yarndevtools.commands.unittestresultaggregator.common import (
    MATCH_ALL_LINES_EXPRESSION,
    REGEX_EVERYTHING,
    MATCHTYPE_ALL_POSTFIX,
    get_key_by_match_expr_and_aggr_filter,
    MatchExpression,
    MatchedLinesFromMessage,
    OperationMode,
)
from yarndevtools.commands.unittestresultaggregator.representation import SummaryGenerator, UnitTestResultOutputManager
from yarndevtools.common.shared_command_utils import SECRET_PROJECTS_DIR
from yarndevtools.constants import UNIT_TEST_RESULT_AGGREGATOR

LOG = logging.getLogger(__name__)

AGGREGATED_WS_POSTFIX = "aggregated"
SUBJECT = "subject:"
DEFAULT_LINE_SEP = "\\r\\n"
MATCH_EXPRESSION_SEPARATOR = "::"
MATCH_EXPRESSION_PATTERN = "^([a-zA-Z]+)%s(.*)$" % MATCH_EXPRESSION_SEPARATOR


class UnitTestResultAggregatorConfig:
    def __init__(self, parser, args, output_dir: str):
        self._validate_args(parser, args)
        self.console_mode = True if "console_mode" in args and args.console_mode else False
        self.gmail_query = args.gmail_query
        self.smart_subject_query = args.smart_subject_query
        self.request_limit = args.request_limit if hasattr(args, "request_limit") and args.request_limit else 1000000
        self.account_email: str = args.account_email
        self.match_expressions: List[MatchExpression] = self._convert_match_expressions(args)
        self.skip_lines_starting_with: List[str] = (
            args.skip_lines_starting_with
            if hasattr(args, "skip_lines_starting_with") and args.skip_lines_starting_with
            else []
        )
        self.email_content_line_sep = (
            args.email_content_line_separator
            if hasattr(args, "email_content_line_separator") and args.email_content_line_separator
            else DEFAULT_LINE_SEP
        )
        self.truncate_subject_with = (
            args.truncate_subject if hasattr(args, "truncate_subject") and args.truncate_subject else None
        )
        self.abbrev_tc_package = (
            args.abbrev_testcase_package
            if hasattr(args, "abbrev_testcase_package") and args.abbrev_testcase_package
            else None
        )
        self.summary_mode = args.summary_mode
        self.aggregate_filters: List[str] = (
            args.aggregate_filters if hasattr(args, "aggregate_filters") and args.aggregate_filters else []
        )
        self.output_dir = output_dir
        self.email_cache_dir = FileUtils.join_path(output_dir, "email_cache")
        self.session_dir = ProjectUtils.get_session_dir_under_child_dir(FileUtils.basename(output_dir))
        self.full_cmd: str = OsUtils.determine_full_command_filtered(filter_password=True)

        if self.operation_mode == OperationMode.GSHEET:
            worksheet_names: List[str] = []
            for match_expr in self.match_expressions + [MATCH_ALL_LINES_EXPRESSION]:
                worksheet_names.append(self.get_worksheet_name(match_expr))
                for aggr_filter in self.aggregate_filters:
                    worksheet_names.append(self.get_worksheet_name(match_expr, aggr_filter))
            LOG.info(f"Generated worksheet names: {worksheet_names}")
            for ws in worksheet_names:
                self.gsheet_options.add_worksheet(ws)

    @staticmethod
    def _convert_match_expressions(args) -> List[MatchExpression]:
        raw_match_exprs: List[str] = (
            args.match_expression if hasattr(args, "match_expression") and args.match_expression else None
        )
        if not raw_match_exprs:
            return [MATCH_ALL_LINES_EXPRESSION]

        match_expressions = []
        for raw_match_expr in raw_match_exprs:
            segments = raw_match_expr.split(MATCH_EXPRESSION_SEPARATOR)
            alias = segments[0]
            if alias == MATCHTYPE_ALL_POSTFIX:
                raise ValueError(
                    f"Alias for match expression '{MATCHTYPE_ALL_POSTFIX}' is reserved. " f"Please use another alias."
                )
            match_expr = segments[1]
            pattern = REGEX_EVERYTHING + match_expr.replace(".", "\\.") + REGEX_EVERYTHING
            match_expressions.append(MatchExpression(alias, raw_match_expr, pattern))
        return match_expressions

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
        valid_op_modes = [OperationMode.PRINT, OperationMode.GSHEET]
        if self.operation_mode not in valid_op_modes:
            raise ValueError(
                f"Unknown state! "
                f"Operation mode should be any of {valid_op_modes}, but it is set to: {self.operation_mode}"
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
            f"Match expressions: {self.match_expressions}\n"
            f"Email line separator: {self.email_content_line_sep}\n"
            f"Request limit: {self.request_limit}\n"
            f"Operation mode: {self.operation_mode}\n"
            f"Skip lines starting with: {self.skip_lines_starting_with}\n"
            f"Truncate subject with: {self.truncate_subject_with}\n"
            f"Abbreviate testcase package: {self.abbrev_tc_package}\n"
            f"Summary mode: {self.summary_mode}\n"
            f"Aggregate filters: {self.aggregate_filters}\n"
        )

    @staticmethod
    def get_worksheet_name(match_expression: MatchExpression, aggr_filter: str = None):
        ws_name: str = f"{match_expression.alias}"
        if aggr_filter:
            ws_name += f"_{aggr_filter}_{AGGREGATED_WS_POSTFIX}"
        else:
            ws_name += f"_{MATCHTYPE_ALL_POSTFIX}"
        return f"{ws_name}"


class TestcaseFilterResults:
    def __init__(self, match_expressions, aggregate_filters):
        self.aggregate_filters = aggregate_filters
        self.match_expressions = match_expressions
        self.match_all_lines: bool = self._should_match_all_lines()
        # Key: Match expression + (Aggregation filter or _ALL match)
        self.all_matches: Dict[str, List[MatchedLinesFromMessage]] = {}

        # This is a temporary dict - usually for a context of a message
        self.matched_lines_dict: Dict[str, List[str]] = {}
        # Key: String key, Value: Tuple of[MatchExpression, aggregation filter]
        self._match_keys: Dict[str, Tuple[MatchExpression, str]] = {}

    def _should_match_all_lines(self):
        match_all_lines: bool = (
            len(self.match_expressions) == 1 and self.match_expressions[0] == MATCH_ALL_LINES_EXPRESSION
        )
        LOG.info(
            "**Matching all lines"
            if match_all_lines
            else f"**Matching lines with regex pattern: {self.match_expressions}"
        )
        return match_all_lines

    def start_new_context(self):
        # Prepare matched_lines dict with all required empty-lists for match expressions and aggregate filters
        self.matched_lines_dict = {MATCHTYPE_ALL_POSTFIX: []}
        for match_expr in self.match_expressions:
            self.matched_lines_dict[self._get_match_key(match_expr)] = []
            for aggr_filter in self.aggregate_filters:
                self.matched_lines_dict[self._get_match_key(match_expr, aggr_filter)] = []

    def match_line(self, line, mail_subject: str):
        matches_any_pattern, matched_expression = self._does_line_match_any_match_expression(line, mail_subject)
        if self.match_all_lines or matches_any_pattern:
            self.matched_lines_dict[MATCHTYPE_ALL_POSTFIX].append(line)
            self.matched_lines_dict[self._get_match_key(matched_expression)].append(line)

            # Check aggregation filters
            for aggr_filter in self.aggregate_filters:
                if aggr_filter in mail_subject:
                    LOG.debug(
                        f"Found matching email subject for aggregation filter '{aggr_filter}': "
                        f"Subject: {mail_subject}"
                    )
                    self.matched_lines_dict[self._get_match_key(matched_expression, aggr_filter)].append(line)

    def _does_line_match_any_match_expression(self, line, mail_subject: str):
        for match_expression in self.match_expressions:
            if RegexUtils.ensure_matches_pattern(line, match_expression.pattern):
                LOG.debug(f"Matched line: {line} [Mail subject: {mail_subject}]")
                return True, match_expression
        LOG.debug(f"Line did not match for any pattern: {line}")
        return False, None

    def _get_match_key(self, match_expr: MatchExpression, aggr_filter: str or None = None) -> str:
        if match_expr == MATCH_ALL_LINES_EXPRESSION:
            self._match_keys[MATCHTYPE_ALL_POSTFIX] = (MATCH_ALL_LINES_EXPRESSION, None)
            return MATCHTYPE_ALL_POSTFIX
        key = get_key_by_match_expr_and_aggr_filter(match_expr, aggr_filter)
        if key not in self._match_keys:
            self._match_keys[key] = (match_expr, aggr_filter)
        return key

    def lookup_match_data_by_key(self, key: str) -> Tuple[MatchExpression, str]:
        return self._match_keys[key]

    def finish_context(self, message: GmailMessage):
        for key, matched_lines in self.matched_lines_dict.items():
            if not matched_lines:
                continue
            match_obj = MatchedLinesFromMessage(
                message.msg_id,
                message.thread_id,
                message.subject,
                message.date,
                matched_lines,
            )
            if key not in self.all_matches:
                self.all_matches[key] = []
            self.all_matches[key].append(match_obj)

        # Make sure temp dict is not used until next cycle
        self.matched_lines_dict = None

    def get_matches_by_criteria(
        self, match_expression: MatchExpression, aggr_filter: str = None
    ) -> List[MatchedLinesFromMessage]:
        key = self._get_match_key(match_expression, aggr_filter=aggr_filter)
        return self.all_matches[key]

    def get_matches_aggregated_all(self) -> Dict[str, List[MatchedLinesFromMessage]]:
        # Remove all keys with '*_ALL'
        filtered_keys = list(filter(lambda x: MATCHTYPE_ALL_POSTFIX.lower() not in x.lower(), self.all_matches.keys()))
        return {key: self.all_matches[key] for key in filtered_keys}

    def print_objects(self):
        LOG.debug(f"All {MatchedLinesFromMessage.__name__} objects: {self.all_matches}")


class UnitTestResultAggregator:
    def __init__(self, args, parser, output_dir: str):
        self.config = UnitTestResultAggregatorConfig(parser, args, output_dir)
        if self.config.operation_mode == OperationMode.GSHEET:
            self.gsheet_wrapper = GSheetWrapper(self.config.gsheet_options)
        self.authorizer = GoogleApiAuthorizer(
            ServiceType.GMAIL,
            project_name=f"{UNIT_TEST_RESULT_AGGREGATOR}",
            secret_basedir=SECRET_PROJECTS_DIR,
            account_email=self.config.account_email,
        )
        self.gmail_wrapper = GmailWrapper(self.authorizer, output_basedir=self.config.email_cache_dir)

    def run(self):
        LOG.info(f"Starting Unit test result aggregator. Config: \n{str(self.config)}")
        gmail_query: str = self._get_gmail_query()
        query_result: ThreadQueryResults = self.gmail_wrapper.query_threads(
            query=gmail_query, limit=self.config.request_limit, expect_one_message_per_thread=True
        )
        LOG.info(f"Received thread query result: {query_result}")
        tc_filter_results: TestcaseFilterResults = self.filter_data_by_match_expressions(query_result)

        output_manager = UnitTestResultOutputManager(
            self.config.session_dir, self.config.console_mode, self.gsheet_wrapper
        )
        SummaryGenerator.process_testcase_filter_results(tc_filter_results, query_result, self.config, output_manager)

    def filter_data_by_match_expressions(self, query_result: ThreadQueryResults) -> TestcaseFilterResults:
        tc_filter_results = TestcaseFilterResults(self.config.match_expressions, self.config.aggregate_filters)
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
        orig_query = self.config.gmail_query
        if self.config.smart_subject_query and orig_query.startswith(SUBJECT):
            after_subject = orig_query.split(SUBJECT)[1]
            matches = [" and ", " or "]
            if any(x in after_subject.lower() for x in matches):
                LOG.warning(f"Detected logical expression in query, won't modify original query: {orig_query}")
                return orig_query
            if " " in after_subject and after_subject[0] != '"':
                fixed_subject = f'"{after_subject}"'
                new_query = SUBJECT + fixed_subject
                LOG.info(
                    f"Fixed gmail query string.\n"
                    f"Original query string: {orig_query}\n"
                    f"New query string: {new_query}"
                )
                return new_query
        return orig_query
