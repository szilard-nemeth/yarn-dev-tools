import logging
from enum import Enum
from typing import List, Any, Dict

from pythoncommons.file_utils import FileUtils
from pythoncommons.result_printer import (
    TabulateTableFormat,
    GenericTableWithHeader,
    ResultPrinter,
    DEFAULT_TABLE_FORMATS,
    TableRenderingConfig,
    BoolConversionConfig,
    Color,
    MatchType,
    ColorDescriptor,
    ColorizeConfig,
    EvaluationMethod,
)
from pythoncommons.string_utils import StringUtils

from yarndevtools.commands.reviewsheetbackportupdater.common import ReviewSheetBackportUpdaterData
from yarndevtools.commands_common import CommitData
from yarndevtools.common.shared_command_utils import HtmlHelper
from yarndevtools.constants import SUMMARY_FILE_TXT, SUMMARY_FILE_HTML, CLOUDERA_CDH_HADOOP_COMMIT_LINK_PREFIX

LOG = logging.getLogger(__name__)


class ReviewSheetBackportUpdaterUpstreamCommitsHeader(Enum):
    ISSUE_NUMBER = "Issue #"
    ISSUE = "Issue"
    BRANCHES = "Branches"
    COMMIT_LINK = "Commit link"
    COMMIT_DATE = "Commit date"
    COMMIT_MESSAGE = "Commit message"


class ReviewSheetBackportUpdaterTable(GenericTableWithHeader):
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


class ReviewSheetBackportUpdaterTableStyle(Enum):
    REGULAR = "regular"
    REGULAR_WITH_COLORS = "regular_colorized"


class ReviewSheetBackportUpdaterTableType(Enum):
    REVIEW_STATUSES = (
        "review_statuses",
        "UPSTREAM REVIEW STATUSES",
        ReviewSheetBackportUpdaterTableStyle.REGULAR_WITH_COLORS,
    )

    def __init__(self, key, header_value, table_type):
        self.key = key
        self.header = header_value
        self.table_type = table_type


class ReviewSheetBackportUpdaterOutputManager:
    LINE_SEPARATOR = "=" * 80

    def __init__(self, config):
        self.config = config

    def print_summary(self, data: ReviewSheetBackportUpdaterData):
        table_data = TableDataPreparator.prepare(data)
        summary_data: ReviewSheetBackportUpdaterSummaryData = ReviewSheetBackportUpdaterSummaryData(self.config, data)
        self.rendered_summary = ReviewSheetBackportUpdaterRenderedSummary(summary_data, table_data, self.config)
        self.print_and_save_summary(self.rendered_summary)

    def print_and_save_summary(self, rendered_summary):
        LOG.info(rendered_summary.printable_summary_str)

        filename = FileUtils.join_path(self.config.session_dir, SUMMARY_FILE_TXT)
        LOG.info(f"Saving summary to text file: {filename}")
        FileUtils.save_to_file(filename, rendered_summary.writable_summary_str)

        filename = FileUtils.join_path(self.config.session_dir, SUMMARY_FILE_HTML)
        LOG.info(f"Saving summary to html file: {filename}")
        FileUtils.save_to_file(filename, rendered_summary.html_summary)


class TableDataPreparator:
    @staticmethod
    def convert_to_hyperlink(link_name, link_value):
        return f'<a href="{link_value}">{link_name}</a>'

    @staticmethod
    def prepare(data):
        rows: List[Any] = []
        for jira_no, backported_jira in enumerate(data.backported_jiras.values()):
            issue_id = backported_jira.jira_id
            single_commit = False
            all_branches = {}
            if issue_id in data.commits_of_jira:
                single_commit = data.is_single_commit(issue_id)
                all_branches = data.backported_to_branches[issue_id]

            if single_commit:
                commit_data = data.get_single_commit(issue_id)
                rows.append(TableDataPreparator._create_row_list(all_branches, commit_data, issue_id, jira_no))
                continue

            no_of_commits = len(backported_jira.commits)
            for backported_commit in backported_jira.commits:
                branches = backported_commit.branches
                if no_of_commits == 1:
                    branches = all_branches
                commit_data: CommitData = backported_commit.commit_obj
                rows.append(TableDataPreparator._create_row_list(branches, commit_data, issue_id, jira_no))
        return rows

    @staticmethod
    def _create_row_list(branches, commit_data, issue_id, jira_no):
        return [
            jira_no + 1,
            issue_id,
            branches,
            TableDataPreparator.convert_to_hyperlink(
                commit_data.hash, CLOUDERA_CDH_HADOOP_COMMIT_LINK_PREFIX + commit_data.hash
            ),
            commit_data.date,
            commit_data.message,
        ]


class ReviewSheetBackportUpdaterRenderedSummary:
    """
    Properties of tables: Table format, RenderedTableType, Branch, Colorized or not.
    - Table format: Normal (regular) / HTML / Any future formats.
    - RenderedTableType: all values of enum.
    - Branch: Some tables are branch-based, e.g. RenderedTableType.UNIQUE_ON_BRANCH
    - Colorized: Bool value indicating if the table values are colorized
    """

    def __init__(self, summary_data, table_data, config):
        self.config = config
        self.summary_data: ReviewSheetBackportUpdaterSummaryData = summary_data
        self.table_data = table_data
        self.table_order: List[ReviewSheetBackportUpdaterTableType] = [
            ReviewSheetBackportUpdaterTableType.REVIEW_STATUSES,
        ]
        self._tables: Dict[ReviewSheetBackportUpdaterTableType, List[ReviewSheetBackportUpdaterTable]] = {}
        self.add_upstream_backports_tables()
        self.printable_summary_str, self.writable_summary_str, self.html_summary = self.generate_summary_msgs()
        LOG.info("Finished rendering all tables")

    def add_upstream_backports_tables(self):
        h = ReviewSheetBackportUpdaterUpstreamCommitsHeader
        header = [
            h.ISSUE_NUMBER.value,
            h.ISSUE.value,
            h.BRANCHES.value,
            h.COMMIT_LINK.value,
            h.COMMIT_DATE.value,
            h.COMMIT_MESSAGE.value,
        ]

        # TODO Fix color conf
        row_len = len(self.table_data[0])
        scan_range = (0, row_len)
        color_conf = ColorizeConfig(
            [
                ColorDescriptor(bool, True, Color.GREEN, MatchType.ALL, scan_range, (0, row_len)),
                ColorDescriptor(bool, False, Color.RED, MatchType.ANY, scan_range, (0, row_len)),
            ],
            eval_method=EvaluationMethod.ALL,
        )

        self._add_review_statuses_table(header, colorize_conf=color_conf)
        self._add_review_statuses_table(header, colorize_conf=None)

    def _add_review_statuses_table(self, header, colorize_conf: ColorizeConfig = None):
        render_conf = TableRenderingConfig(
            row_callback=lambda row: row,
            print_result=False,
            max_width=80,
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
        table_type = ReviewSheetBackportUpdaterTableType.REVIEW_STATUSES
        for table_fmt, table in gen_tables.items():
            self._add_single_table(
                table_type,
                ReviewSheetBackportUpdaterTable(
                    table_type.header,
                    header,
                    self.table_data,
                    table,
                    table_fmt=table_fmt,
                    colorized=True if colorize_conf else False,
                ),
            )

    def _add_single_table(self, ttype: ReviewSheetBackportUpdaterTableType, table: ReviewSheetBackportUpdaterTable):
        if ttype not in self._tables:
            self._tables[ttype] = []
        self._tables[ttype].append(table)

    def generate_summary_msgs(self):
        self.summary_str = self.generate_summary_string()

        def regular_table(table_type: ReviewSheetBackportUpdaterTableType):
            return self.get_tables(table_type, colorized=False, table_fmt=TabulateTableFormat.GRID)

        def html_table(table_type: ReviewSheetBackportUpdaterTableType):
            return self.get_tables(table_type, colorized=False, table_fmt=TabulateTableFormat.HTML)

        def regular_colorized_table(table_type: ReviewSheetBackportUpdaterTableType, colorized=False):
            return self.get_tables(table_type, table_fmt=TabulateTableFormat.GRID, colorized=colorized)

        printable_tables: List[ReviewSheetBackportUpdaterTable] = []
        writable_tables: List[ReviewSheetBackportUpdaterTable] = []
        html_tables: List[ReviewSheetBackportUpdaterTable] = []
        for rtt in self.table_order:
            if rtt.table_type == ReviewSheetBackportUpdaterTableStyle.REGULAR:
                printable_tables.extend(regular_table(rtt))
                writable_tables.extend(regular_table(rtt))
                html_tables.extend(html_table(rtt))
            elif rtt.table_type == ReviewSheetBackportUpdaterTableStyle.REGULAR_WITH_COLORS:
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
        ttype: ReviewSheetBackportUpdaterTableType,
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


class ReviewSheetBackportUpdaterSummaryData:
    def __init__(self, config, data: ReviewSheetBackportUpdaterData):
        self.config = config
        self.output_dir: str = config.output_dir
        self.data = data

    def __str__(self):
        # TODO fix
        res = ""
        return res
