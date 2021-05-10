import copy
import datetime
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Callable, Sized

from googleapiwrapper.common import ServiceType
from googleapiwrapper.gmail_api import GmailWrapper, ThreadQueryResults
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
from pythoncommons.string_utils import RegexUtils, StringUtils, auto_str

from yarndevtools.common.shared_command_utils import SECRET_PROJECTS_DIR
from yarndevtools.constants import UNIT_TEST_RESULT_AGGREGATOR, SUMMARY_FILE_TXT, SUMMARY_FILE_HTML

AGGREGATED_WS_POSTFIX = "_aggregated"

SUBJECT = "subject:"

LOG = logging.getLogger(__name__)

DEFAULT_LINE_SEP = "\\r\\n"
REGEX_EVERYTHING = ".*"
MATCH_ALL_LINES = REGEX_EVERYTHING
ANY_MATCHES_KEY = "any"


class SummaryMode(Enum):
    HTML = "html"
    TEXT = "text"
    ALL = "all"
    NONE = "none"


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
            default_aggr_ws = self.get_default_aggregated_worksheet_name()
            self.aggregated_worksheet_names: Dict[str, str] = {ANY_MATCHES_KEY: default_aggr_ws}
            self.gsheet_options.add_worksheet(default_aggr_ws)
            for aggr_filter in self.aggregate_filters:
                aggr_ws_name = f"{args.gsheet_worksheet}{AGGREGATED_WS_POSTFIX}_{aggr_filter}"
                self.aggregated_worksheet_names[aggr_filter] = aggr_ws_name
                self.gsheet_options.add_worksheet(aggr_ws_name)

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
            f"Output dir: {self.output_dir}\n"
            f"Account email: {self.account_email}\n"
            f"Email cache dir: {self.email_cache_dir}\n"
            f"Session dir: {self.session_dir}\n"
            f"Console mode: {self.console_mode}\n"
            f"Gmail query: {self.gmail_query}\n"
            f"Smart subject query: {self.smart_subject_query}\n"
            f"Match expression: {self.match_expression}\n"
            f"Email line separator: {self.email_content_line_sep}\n"
            f"Request limit: {self.request_limit}\n"
            f"Operation mode: {self.operation_mode}\n"
            f"Skip lines starting with: {self.skip_lines_starting_with}\n"
            f"Truncate subject with: {self.truncate_subject_with}\n"
            f"Abbreviate testcase package: {self.abbrev_tc_package}\n"
            f"Summary mode: {self.summary_mode}\n"
            f"Aggregate filters: {self.aggregate_filters}\n"
        )

    def get_default_aggregated_worksheet_name(self):
        return f"{self.gsheet_options.worksheets[0]}{AGGREGATED_WS_POSTFIX}_{ANY_MATCHES_KEY}"

    def get_default_worksheet_name(self):
        return self.gsheet_options.worksheets[0]


@dataclass
class MatchedLinesFromMessage:
    message_id: str
    thread_id: str
    subject: str
    date: datetime.datetime
    lines: List[str] = field(default_factory=list)


@dataclass
class TestcaseFilterResults:
    matched_in_filters: Dict[str, List[MatchedLinesFromMessage]] = field(default_factory=dict)

    def __post_init__(self):
        self.matched_in_filters[ANY_MATCHES_KEY] = []

    def add_match(self, match_obj: MatchedLinesFromMessage):
        self.matched_in_filters[ANY_MATCHES_KEY].append(match_obj)

    def add_matches(self, matches: List[MatchedLinesFromMessage]):
        self.matched_in_filters[ANY_MATCHES_KEY].extend(matches)

    def add_match_to_post_filter(self, filter_value: str, match: MatchedLinesFromMessage):
        if filter_value not in self.matched_in_filters:
            self.matched_in_filters[filter_value] = []
        self.matched_in_filters[filter_value].append(match)

    def add_matches_to_post_filter(self, filter_value: str, matches: List[MatchedLinesFromMessage]):
        if filter_value not in self.matched_in_filters:
            self.matched_in_filters[filter_value] = []
        self.matched_in_filters[filter_value].extend(matches)

    def get_matches_for_any_thread(self) -> List[MatchedLinesFromMessage]:
        return self.matched_in_filters[ANY_MATCHES_KEY]

    def get_matches_for_filter(self, key: str):
        return self.matched_in_filters[key]

    def get_post_filter_values(self) -> Dict[str, List[MatchedLinesFromMessage]]:
        post_filter_keys = set(self.matched_in_filters.keys())
        post_filter_keys.remove(ANY_MATCHES_KEY)
        return {key: self.matched_in_filters[key] for key in post_filter_keys}

    def get_all_values(self) -> Dict[str, List[MatchedLinesFromMessage]]:
        return self.matched_in_filters


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
        self.output_manager = UnitTestResultOutputManager(self.config.session_dir, self.config.console_mode)

    def run(self):
        LOG.info(f"Starting Unit test result aggregator. Config: \n{str(self.config)}")
        # TODO Split by [] --> Example: org.apache.hadoop.yarn.util.resource.TestResourceCalculator.testDivisionByZeroRatioNumeratorAndDenominatorIsZero[1]
        gmail_query: str = self._get_gmail_query()
        query_result: ThreadQueryResults = self.gmail_wrapper.query_threads(
            query=gmail_query, limit=self.config.request_limit, expect_one_message_per_thread=True
        )
        LOG.info(f"Received thread query result: {query_result}")
        tc_filter_results: TestcaseFilterResults = self.filter_data_by_regex_pattern(query_result)
        self.process_data(tc_filter_results, query_result)

    def filter_data_by_regex_pattern(self, query_result: ThreadQueryResults) -> TestcaseFilterResults:
        match_all_lines: bool = self.config.match_expression == MATCH_ALL_LINES
        LOG.info(
            "**Matching all lines"
            if match_all_lines
            else f"**Matching lines with regex pattern: {self.config.match_expression}"
        )
        tc_filter_results = TestcaseFilterResults()
        for message in query_result.threads.messages:
            msg_parts = message.get_all_plain_text_parts()
            for msg_part in msg_parts:
                lines = msg_part.body_data.split(self.config.email_content_line_sep)
                # Prepare matched_lines dict with all required empty-lists
                matched_lines_dict: Dict[str, List[str]] = {ANY_MATCHES_KEY: []}
                for aggr_filter in self.config.aggregate_filters:
                    matched_lines_dict[aggr_filter] = []

                for line in lines:
                    line = line.strip()
                    # TODO this compiles the pattern over and over again --> Create a new helper function that receives a compiled pattern
                    if not self._check_if_line_is_valid(line, self.config.skip_lines_starting_with):
                        LOG.warning(f"Skipping invalid line: {line} [Mail subject: {message.subject}]")
                        continue
                    if match_all_lines or RegexUtils.ensure_matches_pattern(line, self.config.match_expression):
                        LOG.debug(f"Matched line: {line} [Mail subject: {message.subject}]")
                        matched_lines_dict[ANY_MATCHES_KEY].append(line)
                        # Check aggregation filters
                        for aggr_filter in self.config.aggregate_filters:
                            if aggr_filter in message.subject:
                                LOG.debug(
                                    f"Found matching email subject for aggregation filter '{aggr_filter}': "
                                    f"Subject: {message.subject}"
                                )
                                matched_lines_dict[aggr_filter].append(line)

                for key, matched_lines in matched_lines_dict.items():
                    if not matched_lines:
                        continue
                    match_obj = MatchedLinesFromMessage(
                        message.msg_id,
                        message.thread_id,
                        message.subject,
                        message.date,
                        matched_lines,
                    )
                    if key == ANY_MATCHES_KEY:
                        tc_filter_results.add_match(match_obj)
                    else:
                        tc_filter_results.add_match_to_post_filter(key, match_obj)

        LOG.debug(f"All {MatchedLinesFromMessage.__name__} objects: {tc_filter_results.get_matches_for_any_thread()}")
        return tc_filter_results

    def process_data(self, tc_filter_results: TestcaseFilterResults, query_result: ThreadQueryResults):
        matched_testcases_all_header = ["Date", "Subject", "Testcase", "Message ID", "Thread ID"]
        matched_testcases_aggregated_header = ["Testcase", "Frequency of failures", "Latest failure"]

        if self.config.summary_mode != SummaryMode.NONE.value:
            # TODO fix
            # truncate = self.config.operation_mode == OperationMode.PRINT
            truncate = True if self.config.summary_mode == SummaryMode.TEXT.value else False

            table_renderer = TableRenderer()
            # We apply the specified truncation / abbreviation rules only for TEXT based tables
            # HTML / Gsheet output is just fine with longer names.
            # If SummaryMode.ALL is used, we leave all values intact for simplicity.
            if self.config.abbrev_tc_package or self.config.truncate_subject_with:
                if self.config.summary_mode in [SummaryMode.ALL.value, SummaryMode.HTML.value]:
                    LOG.warning(
                        f"Either abbreviate package or truncate subject is enabled "
                        f"but SummaryMode is set to '{self.config.summary_mode}'. "
                        "Leaving all data intact so truncate / abbreviate options are ignored."
                    )
                    self.config.abbrev_tc_package = None
                    self.config.truncate_subject_with = None

            table_renderer.render_tables(
                header=matched_testcases_all_header,
                data=DataConverter.convert_data_to_rows(
                    tc_filter_results.get_matches_for_any_thread(),
                    truncate_length=truncate,
                    abbrev_tc_package=self.config.abbrev_tc_package,
                    truncate_subject_with=self.config.truncate_subject_with,
                ),
                dtype=TableDataType.MATCHED_LINES,
                formats=DEFAULT_TABLE_FORMATS,
            )

            # Generate table for matched lines (aggregated), including all post-filters
            for key, match_objects in tc_filter_results.get_all_values().items():
                append_to_header_title = f" (filter: {key})" if key != ANY_MATCHES_KEY else None
                table_renderer.render_tables(
                    header=matched_testcases_aggregated_header,
                    append_to_header_title=append_to_header_title,
                    data=DataConverter.convert_data_to_aggregated_rows(
                        tc_filter_results.get_matches_for_filter(key), abbrev_tc_package=self.config.abbrev_tc_package
                    ),
                    dtype=TableDataType.MATCHED_LINES_AGGREGATED,
                    formats=DEFAULT_TABLE_FORMATS,
                    table_alias=key,
                )

            table_renderer.render_tables(
                header=["Subject", "Thread ID"],
                data=DataConverter.convert_email_subjects(query_result),
                dtype=TableDataType.MAIL_SUBJECTS,
                formats=DEFAULT_TABLE_FORMATS,
            )

            table_renderer.render_tables(
                header=["Subject"],
                data=DataConverter.convert_unique_email_subjects(query_result),
                dtype=TableDataType.UNIQUE_MAIL_SUBJECTS,
                formats=DEFAULT_TABLE_FORMATS,
            )

            summary_generator = SummaryGenerator(table_renderer)
            allowed_regular_summary = self.config.summary_mode in [SummaryMode.TEXT.value, SummaryMode.ALL.value]
            allowed_html_summary = self.config.summary_mode in [SummaryMode.HTML.value, SummaryMode.ALL.value]

            if allowed_regular_summary:
                regular_summary: str = summary_generator.generate_summary(
                    self.config.aggregate_filters,
                    TableOutputConfig(TableDataType.MATCHED_LINES, TableOutputFormat.REGULAR),
                    TableOutputConfig(TableDataType.MATCHED_LINES_AGGREGATED, TableOutputFormat.REGULAR),
                    TableOutputConfig(TableDataType.MAIL_SUBJECTS, TableOutputFormat.REGULAR),
                    TableOutputConfig(TableDataType.UNIQUE_MAIL_SUBJECTS, TableOutputFormat.REGULAR),
                )
                self.output_manager.process_regular_summary(regular_summary)

            if allowed_html_summary:
                html_summary: str = summary_generator.generate_summary(
                    self.config.aggregate_filters,
                    TableOutputConfig(TableDataType.MATCHED_LINES, TableOutputFormat.HTML),
                    TableOutputConfig(TableDataType.MATCHED_LINES_AGGREGATED, TableOutputFormat.HTML),
                    TableOutputConfig(TableDataType.MAIL_SUBJECTS, TableOutputFormat.HTML),
                    TableOutputConfig(TableDataType.UNIQUE_MAIL_SUBJECTS, TableOutputFormat.HTML),
                )
                self.output_manager.process_html_summary(html_summary)

            # These should be written regardless of summary-mode settings
            self.output_manager.process_rendered_table_data(table_renderer, TableDataType.MAIL_SUBJECTS)
            self.output_manager.process_rendered_table_data(table_renderer, TableDataType.UNIQUE_MAIL_SUBJECTS)

        if self.config.operation_mode == OperationMode.GSHEET:
            LOG.info("Updating Google sheet with data...")

            # We need to re-generate all the data here, as table renderer might rendered truncated data.
            matched_testcases_data = DataConverter.convert_data_to_rows(
                tc_filter_results.get_matches_for_any_thread(),
                truncate_length=False,
                abbrev_tc_package=None,
                truncate_subject_with=None,
            )
            worksheet_name = self.config.get_default_worksheet_name()
            LOG.info(
                f"Writing GSheet data. "
                f"Worksheet name: {worksheet_name}"
                f"Number of lines will be written: {len(matched_testcases_data)}"
            )
            self.update_gsheet(matched_testcases_all_header, matched_testcases_data, worksheet_name=worksheet_name)

            all_aggregations: List[str] = self.config.aggregate_filters + [ANY_MATCHES_KEY]
            for aggr_key in all_aggregations:
                aggregated_data = DataConverter.convert_data_to_aggregated_rows(
                    tc_filter_results.get_matches_for_filter(aggr_key), abbrev_tc_package=None
                )
                worksheet_name: str = self.config.aggregated_worksheet_names[aggr_key]
                LOG.info(
                    f"Writing GSheet aggregated data for aggregation key '{aggr_key}'. "
                    f"Worksheet name: {worksheet_name}"
                    f"Number of lines will be written: {len(aggregated_data)}"
                )
                self.update_gsheet(
                    matched_testcases_aggregated_header,
                    aggregated_data,
                    worksheet_name=worksheet_name,
                    create_not_existing=True,
                )

    @staticmethod
    def _check_if_line_is_valid(line, skip_lines_starting_with):
        valid_line = True
        for skip_str in skip_lines_starting_with:
            if line.startswith(skip_str):
                valid_line = False
                break
        return valid_line

    def update_gsheet(self, header, data, worksheet_name: str = None, create_not_existing=False):
        self.gsheet_wrapper.write_data(
            header,
            data,
            clear_range=False,
            worksheet_name=worksheet_name,
            create_not_existing_worksheet=create_not_existing,
        )

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


class TableOutputFormat(Enum):
    REGULAR = "regular"
    HTML = "html"
    REGULAR_WITH_COLORS = "regular_colorized"


class TableDataType(Enum):
    MATCHED_LINES = ("matches lines per thread", "MATCHED LINES PER MAIL THREAD")
    MATCHED_LINES_AGGREGATED = ("matches lines aggregated", "MATCHED LINES AGGREGATED")
    MAIL_SUBJECTS = ("found mail subjects", "FOUND MAIL SUBJECTS")
    UNIQUE_MAIL_SUBJECTS = ("found unique mail subjects", "FOUND UNIQUE MAIL SUBJECTS")

    def __init__(self, key, header_value):
        self.key = key
        self.header = header_value


@auto_str
class TableOutputConfig:
    def __init__(self, data_type: TableDataType, table_type: TableOutputFormat):
        self.data_type = data_type
        self.table_type = table_type


class SummaryGenerator:
    def __init__(self, table_renderer):
        self.table_renderer = table_renderer
        self._callback_dict: Dict[TableOutputFormat, Callable] = {
            TableOutputFormat.REGULAR: self._regular_table,
            TableOutputFormat.REGULAR_WITH_COLORS: self._colorized_table,
            TableOutputFormat.HTML: self._html_table,
        }

    def _regular_table(self, dt: TableDataType, alias=None):
        rendered_tables = self.table_renderer.get_tables(
            dt, table_fmt=TabulateTableFormat.GRID, colorized=False, alias=alias
        )
        self._ensure_one_table_found(rendered_tables, dt)
        return rendered_tables[0]

    def _colorized_table(self, dt: TableDataType, alias=None):
        rendered_tables = self.table_renderer.get_tables(
            dt, table_fmt=TabulateTableFormat.GRID, colorized=True, alias=alias
        )
        self._ensure_one_table_found(rendered_tables, dt)
        return rendered_tables[0]

    def _html_table(self, dt: TableDataType, alias=None):
        rendered_tables = self.table_renderer.get_tables(
            dt, table_fmt=TabulateTableFormat.HTML, colorized=False, alias=alias
        )
        self._ensure_one_table_found(rendered_tables, dt)
        return rendered_tables[0]

    @staticmethod
    def _ensure_one_table_found(tables: Sized, dt: TableDataType):
        if not tables:
            raise ValueError(f"Rendered table not found for Table data type: {dt}")
        if len(tables) > 1:
            raise ValueError(
                f"Multiple result tables are found for table data type: {dt}. "
                f"Should have found exactly one table per type."
            )

    def generate_summary(self, aggregate_filters: List[str], *configs: TableOutputConfig) -> str:
        # Validate if TableType is the same for all
        table_types = set([c.table_type for c in configs])
        if len(table_types) > 1:
            raise ValueError(
                f"Provided table configs has different table types, "
                f"they should share the same table type. "
                f"Provided configs: {configs}"
            )
        table_type = list(table_types)[0]

        tables: List[GenericTableWithHeader] = []
        for config in configs:
            alias = None
            if config.data_type == TableDataType.MATCHED_LINES_AGGREGATED:
                alias = ANY_MATCHES_KEY
                for aggr_filter in aggregate_filters:
                    rendered_table = self._callback_dict[table_type](config.data_type, alias=aggr_filter)
                    tables.append(rendered_table)
            rendered_table = self._callback_dict[table_type](config.data_type, alias=alias)
            tables.append(rendered_table)

        if table_type in [TableOutputFormat.REGULAR, TableOutputFormat.REGULAR_WITH_COLORS]:
            return self._generate_final_concat_of_tables(tables)
        elif table_type in [TableOutputFormat.HTML]:
            return self._generate_final_concat_of_tables_html(tables)
        else:
            raise ValueError(f"Invalid state! Table type is not in any of: {[t for t in TableOutputFormat]}")

    @staticmethod
    def _generate_final_concat_of_tables(tables) -> str:
        printable_summary_str: str = ""
        for table in tables:
            printable_summary_str += str(table)
            printable_summary_str += "\n\n"
        return printable_summary_str

    @staticmethod
    def _generate_final_concat_of_tables_html(tables) -> str:
        table_tuples = [(ht.header, ht.table) for ht in tables]
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


# TODO Try to extract this to common class (pythoncommons?), BranchComparator should move to this implementation later.
class TableRenderer:
    def __init__(self):
        self._tables: Dict[str, List[GenericTableWithHeader]] = {}

    def render_tables(
        self,
        header: List[str],
        data: List[List[str]],
        dtype: TableDataType,
        formats: List[TabulateTableFormat],
        colorized=False,
        table_alias=None,
        append_to_header_title=None,
    ) -> Dict[TabulateTableFormat, GenericTableWithHeader]:
        if not formats:
            raise ValueError("Formats should not be empty!")
        rendered_tables: Dict[TabulateTableFormat, str] = ResultPrinter.print_tables(
            data,
            lambda row: row,
            header=header,
            print_result=False,
            max_width=200,
            max_width_separator=" ",
            tabulate_fmts=formats,
        )
        result_dict: Dict[TabulateTableFormat, GenericTableWithHeader] = {}
        for table_fmt, rendered_table in rendered_tables.items():
            header_title = dtype.header
            if append_to_header_title:
                header_title += append_to_header_title
            table_with_header = GenericTableWithHeader(
                header_title, header, data, rendered_table, table_fmt=table_fmt, colorized=colorized
            )
            self._add_table(dtype, table_with_header, alias=table_alias)
            result_dict[table_fmt] = table_with_header
        return result_dict

    def _add_table(self, dtype: TableDataType, table: GenericTableWithHeader, alias=None):
        key = self._generate_key(dtype, alias)
        if key not in self._tables:
            self._tables[key] = []
        self._tables[key].append(table)

    @staticmethod
    def _generate_key(dtype: TableDataType, alias):
        key = dtype.key
        if alias:
            key += f"_{alias}"
        return key

    def get_tables(
        self,
        ttype: TableDataType,
        colorized: bool = False,
        table_fmt: TabulateTableFormat = TabulateTableFormat.GRID,
        alias=None,
    ) -> List[GenericTableWithHeader]:
        key = self._generate_key(ttype, alias)
        return list(filter(lambda t: t.colorized == colorized and t.table_fmt == table_fmt, self._tables[key]))


class UnitTestResultOutputManager:
    def __init__(self, output_dir, console_mode):
        self.output_dir = output_dir
        self.console_mode = console_mode

    def _write_to_configured_destinations(
        self,
        data: str,
        data_type: TableDataType,
        add_sep_to_end=False,
    ):
        """
        Destinations: Console, File or both
        :param data:
        :param add_sep_to_end:
        :return:
        """
        if self.console_mode:
            LOG.info(f"Printing {data_type.key}: {data}")
        else:
            fn_prefix = self._convert_output_type_str_to_file_prefix(data_type.key, add_sep_to_end=add_sep_to_end)
            f = self._generate_filename(self.output_dir, fn_prefix)
            LOG.info(f"Saving {data_type.key} to file: {f}")
            FileUtils.save_to_file(f, data)

    @staticmethod
    def _convert_output_type_str_to_file_prefix(output_type, add_sep_to_end=True):
        file_prefix: str = output_type.replace(" ", "-")
        if add_sep_to_end:
            file_prefix += "-"
        return file_prefix

    @staticmethod
    def _generate_filename(basedir, prefix, branch_name="") -> str:
        return FileUtils.join_path(basedir, f"{prefix}{StringUtils.replace_special_chars(branch_name)}")

    def process_regular_summary(self, rendered_summary: str):
        LOG.info(rendered_summary)
        filename = FileUtils.join_path(self.output_dir, SUMMARY_FILE_TXT)
        LOG.info(f"Saving summary to text file: {filename}")
        FileUtils.save_to_file(filename, rendered_summary)

    def process_html_summary(self, rendered_summary: str):
        # Doesn't make sense to print HTML summary to console
        filename = FileUtils.join_path(self.output_dir, SUMMARY_FILE_HTML)
        LOG.info(f"Saving summary to html file: {filename}")
        FileUtils.save_to_file(filename, rendered_summary)

    def process_normal_table_data(
        self, table_renderer: TableRenderer, data_type: TableDataType, field_separator=" ", row_separator="\n"
    ):
        """
        Processes List of List of strings (table based data). Typically writes data to file or console.
        :param row_separator:
        :param field_separator:
        :param table_renderer:
        :param data_type:
        :return:
        """
        data: List[List[str]] = table_renderer.get_tables(data_type)[0].source_data
        converted_data: str = ""
        for row in data:
            line = field_separator.join(row)
            converted_data += f"{line}{row_separator}"
        self._write_to_configured_destinations(converted_data, data_type)

    def process_rendered_table_data(self, table_renderer: TableRenderer, data_type: TableDataType):
        rendered_table: str = table_renderer.get_tables(data_type)[0].table
        self._write_to_configured_destinations(rendered_table, data_type)


class DataConverter:
    SUBJECT_MAX_LENGTH = 50
    LINE_MAX_LENGTH = 80

    @staticmethod
    def convert_data_to_rows(
        match_objects: List[MatchedLinesFromMessage],
        truncate_length: bool = False,
        truncate_subject_with: str = None,
        abbrev_tc_package: str = None,
    ) -> List[List[str]]:
        data_table: List[List[str]] = []
        truncate_subject: bool = truncate_length
        truncate_lines: bool = truncate_length

        for match_obj in match_objects:
            for testcase_name in match_obj.lines:
                # Don't touch the original MatchObject data.
                # It's not memory efficient to copy subject / TC name but we need the
                # untruncated / original fields later.
                subject = copy.copy(match_obj.subject)
                testcase_name = copy.copy(testcase_name)

                if truncate_subject_with:
                    subject = DataConverter._truncate_subject(subject, truncate_subject_with)
                if abbrev_tc_package:
                    testcase_name = DataConverter._abbreviate_package_name(abbrev_tc_package, testcase_name)

                # Check length-based truncate, if still necessary
                if truncate_subject and len(subject) > DataConverter.SUBJECT_MAX_LENGTH:
                    subject = DataConverter._truncate_str(subject, DataConverter.SUBJECT_MAX_LENGTH, "subject")
                if truncate_lines:
                    testcase_name = DataConverter._truncate_str(
                        testcase_name, DataConverter.LINE_MAX_LENGTH, "testcase"
                    )
                row: List[str] = [
                    str(match_obj.date),
                    subject,
                    testcase_name,
                    match_obj.message_id,
                    match_obj.thread_id,
                ]
                data_table.append(row)
        return data_table

    @staticmethod
    def _abbreviate_package_name(abbrev_tc_package, testcase_name):
        if abbrev_tc_package in testcase_name:
            replacement = ".".join([p[0] for p in abbrev_tc_package.split(".")])
            new_testcase_name = f"{replacement}{testcase_name.split(abbrev_tc_package)[1]}"
            LOG.debug(f"Abbreviated testcase name: '{testcase_name}' -> {new_testcase_name}")
            testcase_name = new_testcase_name
        return testcase_name

    @staticmethod
    def _truncate_subject(subject, truncate_subject_with):
        if truncate_subject_with in subject:
            new_subject = "".join([s for s in subject.split(truncate_subject_with) if s])
            LOG.debug(f"Truncated subject: '{subject}' -> {new_subject}")
            subject = new_subject
        return subject

    @staticmethod
    def convert_data_to_aggregated_rows(
        match_objects: List[MatchedLinesFromMessage], abbrev_tc_package=None
    ) -> List[List[str]]:
        failure_freq: Dict[str, int] = {}
        latest_failure: Dict[str, datetime.datetime] = {}
        for match_obj in match_objects:
            for testcase_name in match_obj.lines:
                if abbrev_tc_package:
                    testcase_name = DataConverter._abbreviate_package_name(abbrev_tc_package, testcase_name)

                if testcase_name not in failure_freq:
                    failure_freq[testcase_name] = 1
                    latest_failure[testcase_name] = match_obj.date
                else:
                    failure_freq[testcase_name] = failure_freq[testcase_name] + 1
                    if match_obj.date > latest_failure[testcase_name]:
                        latest_failure[testcase_name] = match_obj.date

        data_table: List[List[str]] = []
        for testcase, failure_freq in failure_freq.items():
            last_failed = latest_failure[testcase]
            row: List[str] = [testcase, failure_freq, str(last_failed)]
            data_table.append(row)
        return data_table

    @staticmethod
    def convert_email_subjects(query_result: ThreadQueryResults) -> List[List[str]]:
        data_table: List[List[str]] = []
        for tup in query_result.subjects_and_ids:
            data_table.append(list(tup))
        return data_table

    @staticmethod
    def convert_unique_email_subjects(query_result: ThreadQueryResults) -> List[List[str]]:
        return [[subj] for subj in query_result.unique_subjects]

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
