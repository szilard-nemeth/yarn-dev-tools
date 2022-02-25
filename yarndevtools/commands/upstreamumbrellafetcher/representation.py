import os
from enum import Enum
from typing import List, Dict, Any

from pythoncommons.file_utils import FileUtils
from pythoncommons.logging_utils import LoggerFactory
from pythoncommons.result_printer import (
    TabulateTableFormat,
    ResultPrinter,
    DEFAULT_TABLE_FORMATS,
    GenericTableWithHeader,
    TableRenderingConfig,
    ColorizeConfig,
    ColorDescriptor,
    Color,
    MatchType,
    EvaluationMethod,
    BoolConversionConfig,
)
from pythoncommons.string_utils import StringUtils

from yarndevtools.commands.upstreamumbrellafetcher.common import JiraUmbrellaData, ExecutionMode
from yarndevtools.common.shared_command_utils import HtmlHelper
from yarndevtools.constants import SummaryFile

LOG = LoggerFactory.get_logger(__name__)


class UmbrellaFetcherUpstreamCommitsHeader(Enum):
    ROW = "Row"
    JIRA_ID = "Jira ID"
    JIRA_RESOLUTION = "Jira resolution"
    JIRA_STATUS_CATEGORY = "Jira statusCategory"
    COMMIT_HASH = "Commit hash"
    COMMIT_MSG = "Commit message"
    COMMIT_DATE = "Commit date"
    COMMITTER = "Committer"
    BRANCHES = "Branches"


class UmbrellaFetcherResultFilesHeader(Enum):
    ROW = "Row"
    FILE = "File"
    NO_OF_LINES = "# of lines"


class UmbrellaFetcherTable(GenericTableWithHeader):
    def __init__(
        self,
        header_title,
        header: List[str],
        source_data: Any,
        rendered_table: str,
        table_fmt: TabulateTableFormat,
        colorized: bool = False,
    ):
        super().__init__(header_title, header, source_data, rendered_table, table_fmt=table_fmt, colorized=colorized)


class UmbrellaFetcherTableStyle(Enum):
    REGULAR = "regular"
    REGULAR_WITH_COLORS = "regular_colorized"


class UmbrellaFetcherTableType(Enum):
    RESULT_FILES = ("result_files", "RESULT FILES", UmbrellaFetcherTableStyle.REGULAR)
    UPSTREAM_COMMITS_WITH_BACKPORT_DATA = (
        "upstream_commits",
        "UPSTREAM COMMITS WITH BACKPORT DATA",
        UmbrellaFetcherTableStyle.REGULAR_WITH_COLORS,
    )

    def __init__(self, key, header_value, table_type):
        self.key = key
        self.header = header_value
        self.table_type = table_type


class UmbrellaFetcherOutputManager:
    LINE_SEPARATOR = "=" * 80

    def __init__(self, config):
        self.config = config

    def print_and_save_summary(self, rendered_summary):
        LOG.info(rendered_summary.printable_summary_str)

        filename = FileUtils.join_path(self.config.umbrella_result_basedir, SummaryFile.TXT.value)
        LOG.info(f"Saving summary to text file: {filename}")
        FileUtils.save_to_file(filename, rendered_summary.writable_summary_str)

        filename = FileUtils.join_path(self.config.umbrella_result_basedir, SummaryFile.HTML.value)
        LOG.info(f"Saving summary to html file: {filename}")
        FileUtils.save_to_file(filename, rendered_summary.html_summary)


class UmbrellaFetcherRenderedSummary:
    """
    Properties of tables: Table format, RenderedTableType, Branch, Colorized or not.
    - Table format: Normal (regular) / HTML / Any future formats.
    - RenderedTableType: all values of enum.
    - Branch: Some tables are branch-based, e.g. RenderedTableType.UNIQUE_ON_BRANCH
    - Colorized: Bool value indicating if the table values are colorized
    """

    def __init__(self, summary_data, table_data, config):
        self.config = config
        self.summary_data: UmbrellaFetcherSummaryData = summary_data
        self.table_data = table_data
        self.table_order: List[UmbrellaFetcherTableType] = [
            UmbrellaFetcherTableType.UPSTREAM_COMMITS_WITH_BACKPORT_DATA,
            UmbrellaFetcherTableType.RESULT_FILES,
        ]
        self._tables: Dict[UmbrellaFetcherTableType, List[UmbrellaFetcherTable]] = {}
        self.add_upstream_backports_tables()
        self.add_result_files_table()
        self.printable_summary_str, self.writable_summary_str, self.html_summary = self.generate_summary_msgs()
        LOG.info("Finished rendering all tables")

    def add_upstream_backports_tables(self):
        h = UmbrellaFetcherUpstreamCommitsHeader
        if self.config.extended_backport_table:
            header = [h.ROW.value, h.JIRA_ID.value, h.COMMIT_HASH.value, h.COMMIT_MSG.value, h.COMMIT_DATE.value]
        elif self.config.execution_mode in (ExecutionMode.AUTO_BRANCH_MODE, ExecutionMode.MANUAL_BRANCH_MODE):
            header = [h.ROW.value, h.JIRA_ID.value, h.JIRA_RESOLUTION.value, h.JIRA_STATUS_CATEGORY.value]
        else:
            raise ValueError("Unexpected configuration!")
        header.extend(self.config.all_branches_to_consider)

        row_len = len(self.table_data[0])
        color_conf = ColorizeConfig(
            [
                ColorDescriptor(bool, True, Color.GREEN, MatchType.ALL, (0, row_len), (0, row_len)),
                ColorDescriptor(bool, False, Color.RED, MatchType.ANY, (0, row_len), (0, row_len)),
            ],
            eval_method=EvaluationMethod.ALL,
        )

        self._add_upstream_backports_table(header, colorize_conf=color_conf)
        self._add_upstream_backports_table(header, colorize_conf=None)

    def _add_upstream_backports_table(self, header, colorize_conf: ColorizeConfig = None):
        render_conf = TableRenderingConfig(
            row_callback=lambda row: row,
            print_result=False,
            max_width=50,
            max_width_separator=" ",
            tabulate_formats=DEFAULT_TABLE_FORMATS,
            colorize_config=colorize_conf,
            bool_conversion_config=BoolConversionConfig(),
        )
        gen_tables = ResultPrinter.print_tables(
            data=self.table_data,
            header=header,
            render_conf=render_conf,
        )
        table_type = UmbrellaFetcherTableType.UPSTREAM_COMMITS_WITH_BACKPORT_DATA
        for table_fmt, table in gen_tables.items():
            self._add_single_table(
                table_type,
                UmbrellaFetcherTable(
                    table_type.header,
                    header,
                    self.table_data,
                    table,
                    table_fmt=table_fmt,
                    colorized=True if colorize_conf else False,
                ),
            )

    def add_result_files_table(self):
        result_files_data = sorted(
            FileUtils.find_files(self.summary_data.output_dir, regex=".*", full_path_result=True)
        )
        table_type = UmbrellaFetcherTableType.RESULT_FILES
        h = UmbrellaFetcherResultFilesHeader
        header = [h.ROW.value, h.FILE.value, h.NO_OF_LINES.value]

        render_conf = TableRenderingConfig(
            row_callback=lambda file: (file,),
            print_result=False,
            max_width=80,
            max_width_separator=os.sep,
            tabulate_formats=DEFAULT_TABLE_FORMATS,
        )
        gen_tables = ResultPrinter.print_tables(data=result_files_data, header=header, render_conf=render_conf)

        for table_fmt, table in gen_tables.items():
            self._add_single_table(
                table_type,
                UmbrellaFetcherTable(
                    table_type.header,
                    header,
                    result_files_data,
                    table,
                    table_fmt=table_fmt,
                    colorized=False,
                ),
            )

    def _add_single_table(self, ttype: UmbrellaFetcherTableType, table: UmbrellaFetcherTable):
        if ttype not in self._tables:
            self._tables[ttype] = []
        self._tables[ttype].append(table)

    def generate_summary_msgs(self):
        self.summary_str = self.generate_summary_string()

        def regular_table(table_type: UmbrellaFetcherTableType):
            return self.get_tables(table_type, colorized=False, table_fmt=TabulateTableFormat.GRID)

        def html_table(table_type: UmbrellaFetcherTableType):
            return self.get_tables(table_type, colorized=False, table_fmt=TabulateTableFormat.HTML)

        def regular_colorized_table(table_type: UmbrellaFetcherTableType, colorized=False):
            return self.get_tables(table_type, table_fmt=TabulateTableFormat.GRID, colorized=colorized)

        printable_tables: List[UmbrellaFetcherTable] = []
        writable_tables: List[UmbrellaFetcherTable] = []
        html_tables: List[UmbrellaFetcherTable] = []
        for rtt in self.table_order:
            if rtt.table_type == UmbrellaFetcherTableStyle.REGULAR:
                printable_tables.extend(regular_table(rtt))
                writable_tables.extend(regular_table(rtt))
                html_tables.extend(html_table(rtt))
            elif rtt.table_type == UmbrellaFetcherTableStyle.REGULAR_WITH_COLORS:
                printable_tables.extend(regular_colorized_table(rtt, colorized=True))
                writable_tables.extend(regular_colorized_table(rtt, colorized=False))
                html_tables.extend(html_table(rtt))

        return (
            HtmlHelper.generate_summary_str(printable_tables, self.summary_str),
            HtmlHelper.generate_summary_str(writable_tables, self.summary_str),
            HtmlHelper.generate_summary_html(html_tables, self.summary_str),
        )

    def generate_summary_string(self):
        a_normal_table = self.get_tables(list(self.table_order)[0], table_fmt=TabulateTableFormat.GRID)[0]
        length_of_table_first_line = StringUtils.get_first_line_of_multiline_str(a_normal_table.table)
        summary_str = "\n\n" + (
            StringUtils.generate_header_line("SUMMARY", char="═", length=len(length_of_table_first_line)) + "\n"
        )
        summary_str += str(self.summary_data) + "\n\n"
        return summary_str

    def get_tables(
        self,
        ttype: UmbrellaFetcherTableType,
        colorized: bool = False,
        table_fmt: TabulateTableFormat = TabulateTableFormat.GRID,
    ):
        tables = self._tables[ttype]
        found_tables = list(filter(lambda t: t.colorized == colorized and t.table_fmt == table_fmt, tables))
        if not found_tables:
            raise ValueError(
                "Cannot find any table with filter. Table type: {}, Table format: {}, Colorized: {}. "
                "All tables: {}".format(ttype, table_fmt, colorized, self._tables)
            )
        return found_tables


class UmbrellaFetcherSummaryData:
    def __init__(self, config, umbrella_data: JiraUmbrellaData):
        self.config = config
        self.output_dir: str = config.output_dir
        self.umbrella_data = umbrella_data

    def __str__(self):
        res = ""
        res += f"Output dir: {self.output_dir}\n"
        res += f"Config: {str(self.config)}\n"
        res += "\n\n=====Stats=====\n"
        res += f"Number of jiras: {self.umbrella_data.no_of_jiras}\n"
        res += f"Number of files changed: {self.umbrella_data.no_of_files}\n"
        for commits_by_branch in self.umbrella_data.upstream_commits_by_branch.values():
            res += f"Number of commits on branch {commits_by_branch.branch}: {commits_by_branch.no_of_commits}\n"
        return res
