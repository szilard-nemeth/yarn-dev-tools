import copy
import datetime
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Sized, Callable, Any, Tuple

from googleapiwrapper.gmail_api import ThreadQueryResults
from pythoncommons.file_utils import FileUtils
from pythoncommons.html_utils import HtmlGenerator
from pythoncommons.result_printer import (
    TabulateTableFormat,
    GenericTableWithHeader,
    ResultPrinter,
    DEFAULT_TABLE_FORMATS,
)
from pythoncommons.string_utils import StringUtils, auto_str

from yarndevtools.commands.unittestresultaggregator.common import (
    MatchedLinesFromMessage,
    MatchExpression,
    get_key_by_match_expr_and_aggr_filter,
    MATCH_ALL_LINES_EXPRESSION,
    OperationMode,
    SummaryMode,
)
from yarndevtools.constants import SUMMARY_FILE_TXT, SUMMARY_FILE_HTML

LOG = logging.getLogger(__name__)


class TableOutputFormat(Enum):
    REGULAR = "regular"
    HTML = "html"
    REGULAR_WITH_COLORS = "regular_colorized"


class TableDataType(Enum):
    MATCHED_LINES = ("matched lines per thread", "MATCHED LINES PER MAIL THREAD")
    MATCHED_LINES_AGGREGATED = ("matched lines aggregated", "MATCHED LINES AGGREGATED")
    MAIL_SUBJECTS = ("found mail subjects", "FOUND MAIL SUBJECTS")
    UNIQUE_MAIL_SUBJECTS = ("found unique mail subjects", "FOUND UNIQUE MAIL SUBJECTS")

    def __init__(self, key, header_value):
        self.key = key
        self.header = header_value


@dataclass
class OutputFormatRules:
    truncate_length: bool
    abbrev_tc_package: str
    truncate_subject_with: str


@auto_str
class TableRenderingConfig:
    def __init__(
        self,
        data_type: TableDataType,
        match_expressions: List[MatchExpression] or None,
        aggregate_filters: List[str] or None,
        header: List[str],
        table_types: List[TableOutputFormat],
        out_fmt: OutputFormatRules or None,
        table_formats: List[TabulateTableFormat] = DEFAULT_TABLE_FORMATS,
        simple_mode=False,
    ):
        self.table_formats = table_formats
        self.match_expressions = [] if not match_expressions else match_expressions
        self.aggregate_filters = [] if not aggregate_filters else aggregate_filters
        self.header = header
        self.data_type = data_type
        self.table_types = table_types
        self.out_fmt = out_fmt
        self.simple_mode = simple_mode

    def generate_criteria(self) -> List[Tuple[MatchExpression, str]]:
        has_aggr_filters = True if self.aggregate_filters else False
        criteria = []
        for match_expr in self.match_expressions:
            for aggr_filter in self.aggregate_filters:
                criteria.append((match_expr, aggr_filter))
            if not has_aggr_filters:
                criteria.append((match_expr, None))
        return criteria


class SummaryGenerator:
    def __init__(self, table_renderer):
        self.table_renderer = table_renderer
        self._callback_dict: Dict[TableOutputFormat, Callable] = {
            TableOutputFormat.REGULAR: self._regular_table,
            TableOutputFormat.REGULAR_WITH_COLORS: self._colorized_table,
            TableOutputFormat.HTML: self._html_table,
        }

    @staticmethod
    def process_testcase_filter_results(tc_filter_results, query_result: ThreadQueryResults, config, output_manager):
        matched_testcases_all_header = ["Date", "Subject", "Testcase", "Message ID", "Thread ID"]
        matched_testcases_aggregated_header = ["Testcase", "Frequency of failures", "Latest failure"]

        if config.summary_mode != SummaryMode.NONE.value:
            # TODO fix
            # truncate = self.config.operation_mode == OperationMode.PRINT
            truncate = True if config.summary_mode == SummaryMode.TEXT.value else False

            table_renderer = TableRenderer()
            # We apply the specified truncation / abbreviation rules only for TEXT based tables
            # HTML / Gsheet output is just fine with longer names.
            # If SummaryMode.ALL is used, we leave all values intact for simplicity.
            if config.abbrev_tc_package or config.truncate_subject_with:
                if config.summary_mode in [SummaryMode.ALL.value, SummaryMode.HTML.value]:
                    LOG.warning(
                        f"Either abbreviate package or truncate subject is enabled "
                        f"but SummaryMode is set to '{config.summary_mode}'. "
                        "Leaving all data intact so truncate / abbreviate options are ignored."
                    )
                    config.abbrev_tc_package = None
                    config.truncate_subject_with = None

            # Render tables in 2 steps
            # Example scenario:
            # 0 = {MatchExpression} MatchExpression(alias='YARN', original_expression='YARN::org.apache.hadoop.yarn',
            #           pattern='.*org\\.apache\\.hadoop\\.yarn.*')
            # 1 = {MatchExpression} MatchExpression(alias='MR', original_expression='MR::org.apache.hadoop.mapreduce',
            #           pattern='.*org\\.apache\\.hadoop\\.mapreduce.*')
            #
            # Step numbers are in parenthesis
            # Failed testcases_ALL --> Global all (1)
            #
            # Failed testcases_YARN_ALL (1)
            # Failed testcases_YARN_Aggregated_CDPD-7.1x (2)
            # Failed testcases_YARN_Aggregated_CDPD-7.x (2)
            # Failed testcases_MR_ALL (1)
            # Failed testcases_MR_Aggregated_CDPD-7.1x (2)
            # Failed testcases_MR_Aggregated_CDPD-7.x (2)
            render_confs: List[TableRenderingConfig] = [
                # Render tables for all match expressions + ALL values
                # --> 3 tables in case of 2 match expressions
                TableRenderingConfig(
                    data_type=TableDataType.MATCHED_LINES,
                    match_expressions=config.match_expressions + [MATCH_ALL_LINES_EXPRESSION],
                    aggregate_filters=config.aggregate_filters,
                    header=matched_testcases_all_header,
                    table_types=[TableOutputFormat.REGULAR, TableOutputFormat.HTML],
                    out_fmt=OutputFormatRules(truncate, config.abbrev_tc_package, config.truncate_subject_with),
                ),
                # Render tables for all match expressions AND all aggregation filters
                # --> 4 tables in case of 2 match expressions and 2 aggregate filters
                TableRenderingConfig(
                    data_type=TableDataType.MATCHED_LINES_AGGREGATED,
                    match_expressions=config.match_expressions,
                    aggregate_filters=config.aggregate_filters,
                    header=matched_testcases_aggregated_header,
                    table_types=[TableOutputFormat.REGULAR, TableOutputFormat.HTML],
                    out_fmt=OutputFormatRules(False, config.abbrev_tc_package, None),
                ),
                TableRenderingConfig(
                    simple_mode=True,
                    header=["Subject", "Thread ID"],
                    data_type=TableDataType.MAIL_SUBJECTS,
                    table_types=[TableOutputFormat.REGULAR, TableOutputFormat.HTML],
                    aggregate_filters=None,
                    match_expressions=None,
                    out_fmt=None,
                ),
                TableRenderingConfig(
                    simple_mode=True,
                    header=["Subject"],
                    data_type=TableDataType.UNIQUE_MAIL_SUBJECTS,
                    table_types=[TableOutputFormat.REGULAR, TableOutputFormat.HTML],
                    aggregate_filters=None,
                    match_expressions=None,
                    out_fmt=None,
                ),
            ]

            data_dict: Dict[
                TableDataType, Callable[[MatchExpression or None, str or None, OutputFormatRules], List[List[str]]]
            ] = {
                TableDataType.MATCHED_LINES: lambda match_expr, aggr_filter, out_fmt: DataConverter.convert_data_to_rows(
                    tc_filter_results.get_matches_by_criteria(match_expr, aggr_filter),
                    out_fmt,
                ),
                TableDataType.MATCHED_LINES_AGGREGATED: lambda match_expr, aggr_filter, out_fmt: DataConverter.convert_data_to_aggregated_rows(
                    tc_filter_results.get_matches_by_criteria(match_expr, aggr_filter),
                    out_fmt,
                ),
                TableDataType.MAIL_SUBJECTS: lambda match_expr, aggr_filter, out_fmt: DataConverter.convert_email_subjects(
                    query_result
                ),
                TableDataType.UNIQUE_MAIL_SUBJECTS: lambda match_expr, aggr_filter, out_fmt: DataConverter.convert_unique_email_subjects(
                    query_result
                ),
            }
            for render_conf in render_confs:
                table_renderer.render_by_config(render_conf, data_dict[render_conf.data_type])

            summary_generator = SummaryGenerator(table_renderer)
            allowed_regular_summary = config.summary_mode in [SummaryMode.TEXT.value, SummaryMode.ALL.value]
            allowed_html_summary = config.summary_mode in [SummaryMode.HTML.value, SummaryMode.ALL.value]

            if allowed_regular_summary:
                regular_summary: str = summary_generator.generate_summary(render_confs, TableOutputFormat.REGULAR)
                output_manager.process_regular_summary(regular_summary)
            if allowed_html_summary:
                html_summary: str = summary_generator.generate_summary(render_confs, TableOutputFormat.HTML)
                output_manager.process_html_summary(html_summary)

            # These should be written regardless of summary-mode settings
            output_manager.process_rendered_table_data(table_renderer, TableDataType.MAIL_SUBJECTS)
            output_manager.process_rendered_table_data(table_renderer, TableDataType.UNIQUE_MAIL_SUBJECTS)

        if config.operation_mode == OperationMode.GSHEET:
            LOG.info("Updating Google sheet with data...")

            # We need to re-generate all the data here, as table renderer might rendered truncated data.
            for key, match_objects in tc_filter_results.all_matches.items():
                match_expr, aggr_filter = tc_filter_results.lookup_match_data_by_key(key)
                if match_expr == MATCH_ALL_LINES_EXPRESSION or not aggr_filter:
                    match_objects = tc_filter_results.get_matches_by_criteria(match_expr)
                    table_data = DataConverter.convert_data_to_rows(match_objects, OutputFormatRules(False, None, None))
                    data_descriptor = "data"
                    header = matched_testcases_all_header
                else:
                    match_objects = tc_filter_results.get_matches_by_criteria(match_expr, aggr_filter)
                    table_data = DataConverter.convert_data_to_aggregated_rows(
                        match_objects, OutputFormatRules(False, None, None)
                    )
                    data_descriptor = f"aggregated data for aggregation filter {aggr_filter}"
                    header = matched_testcases_aggregated_header
                worksheet_name: str = config.get_worksheet_name(match_expr, aggr_filter)

                LOG.info(
                    f"Writing GSheet {data_descriptor}. "
                    f"Worksheet name: {worksheet_name}"
                    f"Number of lines will be written: {len(table_data)}"
                )
                output_manager.update_gsheet(
                    header, table_data, worksheet_name=worksheet_name, create_not_existing=True
                )

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

    def generate_summary(self, render_confs: List[TableRenderingConfig], table_output_format: TableOutputFormat) -> str:
        tables: List[GenericTableWithHeader] = []
        for conf in render_confs:
            criteria: List[Tuple[MatchExpression, str]] = conf.generate_criteria()
            for c in criteria:
                alias = get_key_by_match_expr_and_aggr_filter(c[0], c[1])
                rendered_table = self._callback_dict[table_output_format](conf.data_type, alias=alias)
                tables.append(rendered_table)
            if conf.simple_mode:
                rendered_table = self._callback_dict[table_output_format](conf.data_type, alias=None)
                tables.append(rendered_table)

        if table_output_format in [TableOutputFormat.REGULAR, TableOutputFormat.REGULAR_WITH_COLORS]:
            return self._generate_final_concat_of_tables(tables)
        elif table_output_format in [TableOutputFormat.HTML]:
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

    def render_by_config(
        self,
        conf: TableRenderingConfig,
        data_callable: Callable[[MatchExpression or None, str or None, OutputFormatRules], List[List[str]]],
    ):
        if conf.simple_mode:
            self._render_tables(
                header=conf.header,
                data=data_callable(None, None, conf.out_fmt),
                dtype=conf.data_type,
                formats=conf.table_formats,
            )
        criteria: List[Tuple[MatchExpression, str]] = conf.generate_criteria()
        for c in criteria:
            match_expr = c[0]
            aggr_filter = c[1]
            key = get_key_by_match_expr_and_aggr_filter(match_expr, aggr_filter)
            self._render_tables(
                header=conf.header,
                data=data_callable(match_expr, aggr_filter, conf.out_fmt),
                dtype=conf.data_type,
                formats=conf.table_formats,
                append_to_header_title=f"_{key}",
                table_alias=key,
            )

    def _render_tables(
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
    def __init__(self, output_dir, console_mode, ghseet_wrapper):
        self.output_dir = output_dir
        self.console_mode = console_mode
        self.gsheet_wrapper = ghseet_wrapper

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

    def update_gsheet(self, header, data, worksheet_name: str = None, create_not_existing=False):
        self.gsheet_wrapper.write_data(
            header,
            data,
            clear_range=False,
            worksheet_name=worksheet_name,
            create_not_existing_worksheet=create_not_existing,
        )


class DataConverter:
    SUBJECT_MAX_LENGTH = 50
    LINE_MAX_LENGTH = 80

    @staticmethod
    def convert_data_to_rows(
        match_objects: List[MatchedLinesFromMessage], out_fmt: OutputFormatRules
    ) -> List[List[str]]:
        data_table: List[List[str]] = []
        truncate_subject: bool = out_fmt.truncate_length
        truncate_lines: bool = out_fmt.truncate_length

        for match_obj in match_objects:
            for testcase_name in match_obj.lines:
                # Don't touch the original MatchObject data.
                # It's not memory efficient to copy subject / TC name but we need the
                # untruncated / original fields later.
                subject = copy.copy(match_obj.subject)
                testcase_name = copy.copy(testcase_name)

                if out_fmt.truncate_subject_with:
                    subject = DataConverter._truncate_subject(subject, out_fmt.truncate_subject_with)
                if out_fmt.abbrev_tc_package:
                    testcase_name = DataConverter._abbreviate_package_name(out_fmt.abbrev_tc_package, testcase_name)

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
        match_objects: List[MatchedLinesFromMessage], out_fmt: OutputFormatRules = None
    ) -> List[List[str]]:
        failure_freq: Dict[str, int] = {}
        latest_failure: Dict[str, datetime.datetime] = {}
        failure_dates_per_testcase: Dict[str, List[datetime.datetime]]
        for match_obj in match_objects:
            for testcase_name in match_obj.lines:
                if out_fmt.abbrev_tc_package:
                    testcase_name = DataConverter._abbreviate_package_name(out_fmt.abbrev_tc_package, testcase_name)

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
