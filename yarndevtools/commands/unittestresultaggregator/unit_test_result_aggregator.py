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
from pythoncommons.html_utils import HtmlGenerator
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils
from pythoncommons.result_printer import (
    ResultPrinter,
    TabulateTableFormat,
    DEFAULT_TABLE_FORMATS,
    GenericTableWithHeader,
)
from pythoncommons.string_utils import RegexUtils, StringUtils

from yarndevtools.common.shared_command_utils import SECRET_PROJECTS_DIR
from yarndevtools.constants import UNIT_TEST_RESULT_AGGREGATOR, SUMMARY_FILE_TXT, SUMMARY_FILE_HTML

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
        self.console_mode = True if "console_mode" in args and args.console_mode else False
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
            f"Console mode: {self.console_mode}\n"
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
        gmail_query: str = self._get_gmail_query()
        threads: GmailThreads = self.gmail_wrapper.query_threads_with_paging(
            query=gmail_query, limit=self.config.request_limit, expect_one_message_per_thread=True
        )
        match_objects: List[MatchedLinesFromMessage] = self.filter_data_by_regex_pattern(threads)
        self.process_data(match_objects)

    def filter_data_by_regex_pattern(self, threads) -> List[MatchedLinesFromMessage]:
        matched_lines_from_message_objs: List[MatchedLinesFromMessage] = []
        match_all_lines: bool = self.config.match_expression == MATCH_ALL_LINES
        LOG.info(
            "**Matching all lines"
            if match_all_lines
            else f"**Matching lines with regex pattern: {self.config.match_expression}"
        )
        for message in threads.messages:
            msg_parts = message.get_all_plain_text_parts()
            for msg_part in msg_parts:
                lines = msg_part.body_data.split(self.config.email_content_line_sep)
                matched_lines: List[str] = []
                for line in lines:
                    line = line.strip()
                    # TODO this compiles the pattern over and over again --> Create a new helper function that receives a compiled pattern
                    if not self._check_if_line_is_valid(line, self.config.skip_lines_starting_with):
                        LOG.warning(f"Skipping invalid line: {line} [Mail subject: {message.subject}]")
                        continue
                    if match_all_lines or RegexUtils.ensure_matches_pattern(line, self.config.match_expression):
                        LOG.debug(f"Matched line: {line} [Mail subject: {message.subject}]")
                        matched_lines.append(line)
                matched_lines_from_message_objs.append(
                    MatchedLinesFromMessage(
                        message.msg_id, message.thread_id, message.subject, message.date, matched_lines
                    )
                )
        LOG.debug(f"All {MatchedLinesFromMessage.__name__} objects: {matched_lines_from_message_objs}")
        return matched_lines_from_message_objs

    def process_data(self, match_objects: List[MatchedLinesFromMessage]):
        truncate = self.config.operation_mode == OperationMode.PRINT
        normal_table_header = ["Date", "Subject", "Testcase", "Message ID", "Thread ID"]
        aggregated_table_header = ["Testcase", "Frequency of failures", "Latest failure"]
        simple_match_result_rows: List[List[str]] = DataConverter.convert_data_to_rows(match_objects, truncate=truncate)
        aggregated_match_result_rows: List[List[str]] = DataConverter.convert_data_to_aggregated_rows(match_objects)

        table_config = (
            TableConfig()
            .add(TableDataType.MATCHED_LINES, [TableType.REGULAR, TableType.HTML])
            .add(TableDataType.MATCHED_LINES_AGGREGATED, [TableType.REGULAR, TableType.HTML])
        )
        rendered_summary = RenderedSummary(
            normal_table_header,
            simple_match_result_rows,
            aggregated_table_header,
            aggregated_match_result_rows,
            table_config,
        )
        output_manager = UnitTestResultOutputManager(self.config.session_dir, self.config.console_mode)
        output_manager.print_and_save_summary(rendered_summary)

        if self.config.operation_mode == OperationMode.GSHEET:
            LOG.info("Updating Google sheet with data...")
            self.update_gsheet(normal_table_header, simple_match_result_rows)
            self.update_gsheet_aggregated(aggregated_table_header, aggregated_match_result_rows)

    @staticmethod
    def _check_if_line_is_valid(line, skip_lines_starting_with):
        valid_line = True
        for skip_str in skip_lines_starting_with:
            if line.startswith(skip_str):
                valid_line = False
                break
        return valid_line

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
        return orig_query


class TableType(Enum):
    REGULAR = "regular"
    HTML = "html"
    REGULAR_WITH_COLORS = "regular_colorized"


class TableDataType(Enum):
    MATCHED_LINES = ("matches lines per thread", "MATCHED LINES PER MAIL THREAD")
    MATCHED_LINES_AGGREGATED = ("matches lines aggregated", "MATCHED LINES AGGREGATED")

    def __init__(self, key, header_value):
        self.key = key
        self.header = header_value


class TableConfig:
    def __init__(self):
        self.tables: Dict[TableDataType, List[TableType]] = {}

    def add(self, data_type, table_type):
        if data_type not in self.tables:
            self.tables[data_type] = []
        self.tables[data_type].extend(table_type)
        return self


class RenderedSummary:
    def __init__(
        self,
        normal_table_header: List[str],
        simple_match_result_rows: List[List[str]],
        aggregated_table_header: List[str],
        aggregated_match_result_rows: List[List[str]],
        table_config: TableConfig,
    ):
        self.simple_table_header = normal_table_header
        self.simple_match_result_rows = simple_match_result_rows
        self.aggregated_table_header = aggregated_table_header
        self.aggregated_match_result_rows = aggregated_match_result_rows
        self.table_config = table_config

        self._tables: Dict[TableDataType, List[GenericTableWithHeader]] = {}
        self.add_matched_lines_result_table()
        self.add_matched_lines_result_table_aggregated()
        self.printable_summary_str, self.html_summary = self.generate_summary_msgs()

    @property
    def writable_summary_str(self):
        # Writable / Printable tables are the same: no colorization implementation yet
        return self.printable_summary_str

    def get_tables(
        self, ttype: TableDataType, colorized: bool = False, table_fmt: TabulateTableFormat = TabulateTableFormat.GRID
    ):
        tables = self._tables[ttype]
        return list(filter(lambda t: t.colorized == colorized and t.table_fmt == table_fmt, tables))

    def add_matched_lines_result_table(self):
        self._add_table_internal(self.simple_table_header, self.simple_match_result_rows, TableDataType.MATCHED_LINES)

    def add_matched_lines_result_table_aggregated(self):
        self._add_table_internal(
            self.aggregated_table_header, self.aggregated_match_result_rows, TableDataType.MATCHED_LINES_AGGREGATED
        )

    def _add_table_internal(self, header: List[str], data: List[List[str]], dtype: TableDataType):
        gen_tables = ResultPrinter.print_tables(
            data,
            lambda row: row,
            header=header,
            print_result=False,
            max_width=200,
            max_width_separator=" ",
            tabulate_fmts=DEFAULT_TABLE_FORMATS,
        )
        for table_fmt, table in gen_tables.items():
            self._add_table(dtype, GenericTableWithHeader(dtype.header, table, table_fmt=table_fmt, colorized=False))

    def _add_table(self, dtype: TableDataType, table: GenericTableWithHeader):
        if dtype not in self._tables:
            self._tables[dtype] = []
        self._tables[dtype].append(table)

    def generate_summary_msgs(self):
        def regular_table(dt: TableDataType):
            return self.get_tables(dt, table_fmt=TabulateTableFormat.GRID, colorized=False)

        def regular_colorized_table(dt: TableDataType):
            return self.get_tables(dt, table_fmt=TabulateTableFormat.GRID, colorized=True)

        def html_table(dt: TableDataType):
            return self.get_tables(dt, table_fmt=TabulateTableFormat.HTML, colorized=False)

        printable_tables: List[GenericTableWithHeader] = []
        html_tables: List[GenericTableWithHeader] = []
        for table_data_type, table_types in self.table_config.tables.items():
            for tt in table_types:
                if tt == TableType.REGULAR:
                    printable_tables.extend(regular_table(table_data_type))
                elif tt == TableType.HTML:
                    html_tables.extend(html_table(table_data_type))
                elif tt == TableType.REGULAR_WITH_COLORS:
                    printable_tables.extend(regular_colorized_table(table_data_type))
        return (
            self._generate_summary_str(printable_tables),
            self.generate_summary_html(html_tables),
        )

    @staticmethod
    def _generate_summary_str(tables):
        printable_summary_str: str = ""
        for table in tables:
            printable_summary_str += str(table)
            printable_summary_str += "\n\n"
        return printable_summary_str

    @staticmethod
    def generate_summary_html(html_tables) -> str:
        table_tuples = [(h.header, h.table) for h in html_tables]

        html_sep = HtmlGenerator.generate_separator(tag="hr", breaks=2)
        return (
            HtmlGenerator()
            .begin_html_tag()
            .add_basic_table_style()
            .append_html_tables(
                table_tuples, separator=html_sep, header_type="h1", additional_separator_at_beginning=True
            )
            .render()
        )


class UnitTestResultOutputManager:
    def __init__(self, output_dir, console_mode):
        self.output_dir = output_dir
        self.console_mode = console_mode

    def _write_to_file_or_console(self, contents, output_type, add_sep_to_end=False):
        if self.console_mode:
            LOG.info(f"Printing {output_type}: {contents}")
        else:
            fn_prefix = self._convert_output_type_str_to_file_prefix(output_type, add_sep_to_end=add_sep_to_end)
            f = self._generate_filename(self.output_dir, fn_prefix)
            LOG.info(f"Saving {output_type} to file: {f}")
            FileUtils.save_to_file(f, contents)

    def print_and_save_summary(self, rendered_summary: RenderedSummary):
        LOG.info(rendered_summary.printable_summary_str)

        filename = FileUtils.join_path(self.output_dir, SUMMARY_FILE_TXT)
        LOG.info(f"Saving summary to text file: {filename}")
        FileUtils.save_to_file(filename, rendered_summary.writable_summary_str)

        filename = FileUtils.join_path(self.output_dir, SUMMARY_FILE_HTML)
        LOG.info(f"Saving summary to html file: {filename}")
        FileUtils.save_to_file(filename, rendered_summary.html_summary)

    @staticmethod
    def _convert_output_type_str_to_file_prefix(output_type, add_sep_to_end=True):
        file_prefix: str = output_type.replace(" ", "-")
        if add_sep_to_end:
            file_prefix += "-"
        return file_prefix

    @staticmethod
    def _generate_filename(basedir, prefix, branch_name="") -> str:
        return FileUtils.join_path(basedir, f"{prefix}{StringUtils.replace_special_chars(branch_name)}")


class DataConverter:
    SUBJECT_MAX_LENGTH = 50
    LINE_MAX_LENGTH = 80

    @staticmethod
    def convert_data_to_rows(match_objects: List[MatchedLinesFromMessage], truncate: bool = False) -> List[List[str]]:
        converted_data: List[List[str]] = []
        truncate_subject: bool = truncate
        truncate_lines: bool = truncate

        for matched_lines in match_objects:
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
