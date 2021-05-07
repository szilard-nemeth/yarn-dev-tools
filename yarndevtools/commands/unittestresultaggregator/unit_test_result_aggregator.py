import copy
import datetime
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict

from googleapiwrapper.common import ServiceType
from googleapiwrapper.gmail_api import GmailWrapper, GmailThreads
from googleapiwrapper.google_auth import GoogleApiAuthorizer
from googleapiwrapper.google_sheet import GSheetOptions, GSheetWrapper
from pythoncommons.file_utils import FileUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils
from pythoncommons.result_printer import BasicResultPrinter
from pythoncommons.string_utils import RegexUtils

from yarndevtools.common.shared_command_utils import SECRET_PROJECTS_DIR
from yarndevtools.constants import UNIT_TEST_RESULT_AGGREGATOR

SUBJECT = "subject:"

LOG = logging.getLogger(__name__)

DEFAULT_LINE_SEP = "\\r\\n"
REGEX_EVERYTHING = ".*"
MATCH_ALL_LINES = REGEX_EVERYTHING


class OperationMode(Enum):
    GSHEET = "GSHEET"
    PRINT = "PRINT"


class UnitTestResultAggregatorConfig:
    def __init__(self, parser, args, output_dir: str):
        self._validate_args(parser, args)
        self.gmail_query = args.gmail_query
        self.smart_subject_query = args.smart_subject_query
        self.request_limit = args.request_limit if hasattr(args, "request_limit") and args.request_limit else 1000000
        self.account_email: str = args.account_email
        self.match_expression = self._convert_match_expression(args)
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
        self.output_dir = output_dir
        self.email_cache_dir = FileUtils.join_path(output_dir, "email_cache")
        self.session_dir = ProjectUtils.get_session_dir_under_child_dir(FileUtils.basename(output_dir))
        self.full_cmd: str = OsUtils.determine_full_command_filtered(filter_password=True)

    @staticmethod
    def _convert_match_expression(args):
        raw_match_expr = args.match_expression if hasattr(args, "match_expression") and args.match_expression else None
        if not raw_match_expr:
            return MATCH_ALL_LINES
        match_expression = REGEX_EVERYTHING + raw_match_expr.replace(".", "\\.") + REGEX_EVERYTHING
        return match_expression

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
            self.gsheet_options = GSheetOptions(
                args.gsheet_client_secret, args.gsheet_spreadsheet, args.gsheet_worksheet
            )
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
            f"Gmail query: {self.gmail_query}\n"
            f"Smart subject query: {self.smart_subject_query}\n"
            f"Match expression: {self.match_expression}\n"
            f"Email line separator: {self.email_content_line_sep}\n"
            f"Request limit: {self.request_limit}\n"
            f"Operation mode: {self.operation_mode}\n"
            f"Skip lines starting with: {self.skip_lines_starting_with}\n"
        )


@dataclass
class MatchedLinesFromMessage:
    message_id: str
    thread_id: str
    subject: str
    date: datetime.datetime
    lines: List[str] = field(default_factory=list)


class UnitTestResultAggregator:
    def __init__(self, args, parser, output_dir: str):
        self.config = UnitTestResultAggregatorConfig(parser, args, output_dir)
        if self.config.operation_mode == OperationMode.GSHEET:
            self.gsheet_wrapper_normal = GSheetWrapper(self.config.gsheet_options)
            gsheet_options = copy.copy(self.config.gsheet_options)
            gsheet_options.worksheet = gsheet_options.worksheet + "_aggregated"
            self.gsheet_wrapper_aggregated = GSheetWrapper(gsheet_options)
        self.authorizer = GoogleApiAuthorizer(
            ServiceType.GMAIL,
            project_name=f"{UNIT_TEST_RESULT_AGGREGATOR}",
            secret_basedir=SECRET_PROJECTS_DIR,
            account_email=self.config.account_email,
        )
        self.gmail_wrapper = GmailWrapper(self.authorizer, output_basedir=self.config.email_cache_dir)

    def run(self):
        LOG.info(f"Starting Unit test result aggregator. Config: \n{str(self.config)}")
        # TODO Split by [] --> Example: org.apache.hadoop.yarn.util.resource.TestResourceCalculator.testDivisionByZeroRatioNumeratorAndDenominatorIsZero[1]

        # TODO These queries below produced some errors: Uncomment & try again
        # query = "YARN Daily branch diff report"
        # query = "subject: YARN Daily branch diff report"

        gmail_query: str = self._get_gmail_query()
        threads: GmailThreads = self.gmail_wrapper.query_threads_with_paging(
            query=gmail_query, limit=self.config.request_limit, expect_one_message_per_thread=True
        )
        raw_data = self.filter_data_by_regex_pattern(threads)
        self.process_data(raw_data)

    def filter_data_by_regex_pattern(self, threads):
        matched_lines: List[MatchedLinesFromMessage] = []
        match_all_lines: bool = self.config.match_expression == MATCH_ALL_LINES
        LOG.info(
            "Matching all lines" if match_all_lines else f"Matching lines with regex: {self.config.match_expression}"
        )
        for message in threads.messages:
            msg_parts = message.get_all_plain_text_parts()
            for msg_part in msg_parts:
                lines = msg_part.body.split(self.config.email_content_line_sep)
                matched_lines_of_msg: List[str] = []
                for line in lines:
                    line = line.strip()
                    # TODO this compiles the pattern over and over again --> Create a new helper function that receives a compiled pattern
                    if not self._check_if_line_is_valid(line, self.config.skip_lines_starting_with):
                        LOG.warning(f"Skipping invalid line: {line}")
                        continue
                    if match_all_lines:
                        LOG.debug(f"Matched line (match all=True): {line}")
                        matched_lines_of_msg.append(line)
                    elif RegexUtils.ensure_matches_pattern(line, self.config.match_expression):
                        LOG.debug(f"[PATTERN: {self.config.match_expression}] Matched line: {line}")
                        matched_lines_of_msg.append(line)

                matched_lines.append(
                    MatchedLinesFromMessage(
                        message.msg_id, message.thread_id, message.subject, message.date, matched_lines_of_msg
                    )
                )
        LOG.debug(f"[RAW DATA] Matched lines: {matched_lines}")
        return matched_lines

    def process_data(self, raw_data: List[MatchedLinesFromMessage]):
        truncate = self.config.operation_mode == OperationMode.PRINT
        header = ["Date", "Subject", "Testcase", "Message ID", "Thread ID"]
        converted_data: List[List[str]] = DataConverter.convert_data_to_rows(raw_data, truncate=truncate)
        self.print_results_table(header, converted_data)

        if self.config.operation_mode == OperationMode.GSHEET:
            LOG.info("Updating Google sheet with data...")
            header_aggregated = ["Testcase", "Frequency of failures", "Latest failure"]
            aggregated_data: List[List[str]] = DataConverter.convert_data_to_aggregated_rows(raw_data)
            self.update_gsheet(header, converted_data)
            self.update_gsheet_aggregated(header_aggregated, aggregated_data)

    @staticmethod
    def _check_if_line_is_valid(line, skip_lines_starting_with):
        valid_line = True
        for skip_str in skip_lines_starting_with:
            if line.startswith(skip_str):
                valid_line = False
                break
        return valid_line

    @staticmethod
    def print_results_table(header, data):
        BasicResultPrinter.print_table(data, header)

    def update_gsheet(self, header, data):
        self.gsheet_wrapper_normal.write_data(header, data, clear_range=False)

    def update_gsheet_aggregated(self, header, data):
        self.gsheet_wrapper_aggregated.write_data(header, data, clear_range=False)

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


class DataConverter:
    SUBJECT_MAX_LENGTH = 50
    LINE_MAX_LENGTH = 80

    @staticmethod
    def convert_data_to_rows(raw_data: List[MatchedLinesFromMessage], truncate: bool = False) -> List[List[str]]:
        converted_data: List[List[str]] = []
        truncate_subject: bool = truncate
        truncate_lines: bool = truncate

        for matched_lines in raw_data:
            for testcase_name in matched_lines.lines:
                subject = matched_lines.subject
                if truncate_subject and len(matched_lines.subject) > DataConverter.SUBJECT_MAX_LENGTH:
                    subject = DataConverter._truncate_str(
                        matched_lines.subject, DataConverter.SUBJECT_MAX_LENGTH, "subject"
                    )
                if truncate_lines:
                    testcase_name = DataConverter._truncate_str(
                        testcase_name, DataConverter.LINE_MAX_LENGTH, "testcase"
                    )
                row: List[str] = [
                    str(matched_lines.date),
                    subject,
                    testcase_name,
                    matched_lines.message_id,
                    matched_lines.thread_id,
                ]
                converted_data.append(row)
        return converted_data

    @staticmethod
    def convert_data_to_aggregated_rows(raw_data: List[MatchedLinesFromMessage]) -> List[List[str]]:
        failure_freq: Dict[str, int] = {}
        latest_failure: Dict[str, datetime.datetime] = {}
        for matched_lines in raw_data:
            for testcase_name in matched_lines.lines:
                if testcase_name not in failure_freq:
                    failure_freq[testcase_name] = 1
                    latest_failure[testcase_name] = matched_lines.date
                else:
                    failure_freq[testcase_name] = failure_freq[testcase_name] + 1
                    if latest_failure[testcase_name] < matched_lines.date:
                        latest_failure[testcase_name] = matched_lines.date

        converted_data: List[List[str]] = []
        for tc, freq in failure_freq.items():
            last_failed = latest_failure[tc]
            row: List[str] = [tc, freq, str(last_failed)]
            converted_data.append(row)
        return converted_data

    @staticmethod
    def _truncate_str(value: str, max_len: int, field_name: str):
        orig_value = value
        truncated = value[0:max_len] + "..."
        LOG.debug(
            f"Truncated {field_name}: "
            f"Original value: '{orig_value}', "
            f"Original length: {len(orig_value)}, "
            f"New value (truncated): {truncated}, "
            f"New length: {max_len}"
        )
        return truncated

    @staticmethod
    def _truncate_date(date):
        original_date = date
        date_obj = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
        truncated = date_obj.strftime("%Y-%m-%d")
        LOG.debug(f"Truncated date. " f"Original value: {original_date}," f"New value (truncated): {truncated}")
        return truncated
