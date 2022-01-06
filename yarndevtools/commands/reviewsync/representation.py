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

from yarndevtools.commands.reviewsync.common import ReviewsyncData
from yarndevtools.common.shared_command_utils import HtmlHelper
from yarndevtools.constants import SUMMARY_FILE_TXT, SUMMARY_FILE_HTML

LOG = logging.getLogger(__name__)


class ReviewsyncUpstreamCommitsHeader(Enum):
    ROW = "Row"
    ISSUE = "Issue"
    PATCH_APPLY = "Patch apply"
    OWNER = "Owner"
    PATCH_FILE = "Patch file"
    BRANCH = "Branch"
    EXPLICIT = "Explicit"
    RESULT = "Result"
    NUMBER_OF_CONFLICTED_FILES = "Number of conflicted files"
    OVERALL_RESULT = "Overall result"


class ReviewsyncResultFilesHeader(Enum):
    ROW = "Row"
    FILE = "File"
    NO_OF_LINES = "# of lines"


class ReviewsyncTable(GenericTableWithHeader):
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


class ReviewsyncTableStyle(Enum):
    REGULAR = "regular"
    REGULAR_WITH_COLORS = "regular_colorized"


class ReviewsyncTableType(Enum):
    REVIEW_STATUSES = (
        "review_statuses",
        "UPSTREAM REVIEW STATUSES",
        ReviewsyncTableStyle.REGULAR_WITH_COLORS,
    )

    def __init__(self, key, header_value, table_type):
        self.key = key
        self.header = header_value
        self.table_type = table_type


class ReviewSyncOutputManager:
    LINE_SEPARATOR = "=" * 80

    def __init__(self, config):
        self.config = config

    def print_summary(self, data: ReviewsyncData):
        table_data = TableDataPreparator.prepare(data)
        summary_data: ReviewsyncSummaryData = ReviewsyncSummaryData(self.config, data)
        self.rendered_summary = ReviewsyncRenderedSummary(summary_data, table_data, self.config)
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
    def prepare(data):
        rows: List[Any] = []
        row_number = 0
        for issue_id, patch_applies in data.patch_applies_for_issues.items():
            for idx, patch_apply in enumerate(patch_applies):
                row_number += 1
                patch = patch_apply.patch
                explicit = "Yes" if patch_apply.explicit else "No"
                conflicts = "N/A" if patch_apply.conflicts == 0 else str(patch_apply.conflicts)
                if patch:
                    owner = patch.owner_display_name
                    filename = patch.filename
                    status = patch.overall_status.status
                else:
                    owner = "N/A"
                    filename = "N/A"
                    status = "N/A"
                rows.append(
                    [
                        row_number,
                        issue_id,
                        idx + 1,
                        owner,
                        filename,
                        patch_apply.branch,
                        explicit,
                        patch_apply.result,
                        conflicts,
                        status,
                    ]
                )

        return rows


class ReviewsyncRenderedSummary:
    """
    Properties of tables: Table format, RenderedTableType, Branch, Colorized or not.
    - Table format: Normal (regular) / HTML / Any future formats.
    - RenderedTableType: all values of enum.
    - Branch: Some tables are branch-based, e.g. RenderedTableType.UNIQUE_ON_BRANCH
    - Colorized: Bool value indicating if the table values are colorized
    """

    def __init__(self, summary_data, table_data, config):
        self.config = config
        self.summary_data: ReviewsyncSummaryData = summary_data
        self.table_data = table_data
        self.table_order: List[ReviewsyncTableType] = [
            ReviewsyncTableType.REVIEW_STATUSES,
        ]
        self._tables: Dict[ReviewsyncTableType, List[ReviewsyncTable]] = {}
        self.add_upstream_backports_tables()
        self.printable_summary_str, self.writable_summary_str, self.html_summary = self.generate_summary_msgs()
        LOG.info("Finished rendering all tables")

    def add_upstream_backports_tables(self):
        h = ReviewsyncUpstreamCommitsHeader
        header = [
            h.ROW.value,
            h.ISSUE.value,
            h.PATCH_APPLY.value,
            h.OWNER.value,
            h.PATCH_FILE.value,
            h.BRANCH.value,
            h.EXPLICIT.value,
            h.RESULT.value,
            h.NUMBER_OF_CONFLICTED_FILES.value,
            h.OVERALL_RESULT.value,
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
        table_type = ReviewsyncTableType.REVIEW_STATUSES
        for table_fmt, table in gen_tables.items():
            self._add_single_table(
                table_type,
                ReviewsyncTable(
                    table_type.header,
                    header,
                    self.table_data,
                    table,
                    table_fmt=table_fmt,
                    colorized=True if colorize_conf else False,
                ),
            )

    def _add_single_table(self, ttype: ReviewsyncTableType, table: ReviewsyncTable):
        if ttype not in self._tables:
            self._tables[ttype] = []
        self._tables[ttype].append(table)

    def generate_summary_msgs(self):
        self.summary_str = self.generate_summary_string()

        def regular_table(table_type: ReviewsyncTableType):
            return self.get_tables(table_type, colorized=False, table_fmt=TabulateTableFormat.GRID)

        def html_table(table_type: ReviewsyncTableType):
            return self.get_tables(table_type, colorized=False, table_fmt=TabulateTableFormat.HTML)

        def regular_colorized_table(table_type: ReviewsyncTableType, colorized=False):
            return self.get_tables(table_type, table_fmt=TabulateTableFormat.GRID, colorized=colorized)

        printable_tables: List[ReviewsyncTable] = []
        writable_tables: List[ReviewsyncTable] = []
        html_tables: List[ReviewsyncTable] = []
        for rtt in self.table_order:
            if rtt.table_type == ReviewsyncTableStyle.REGULAR:
                printable_tables.extend(regular_table(rtt))
                writable_tables.extend(regular_table(rtt))
                html_tables.extend(html_table(rtt))
            elif rtt.table_type == ReviewsyncTableStyle.REGULAR_WITH_COLORS:
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
        ttype: ReviewsyncTableType,
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


class ReviewsyncSummaryData:
    def __init__(self, config, reviewsync_data: ReviewsyncData):
        self.config = config
        self.output_dir: str = config.output_dir
        self.reviewsync_data = reviewsync_data

    def __str__(self):
        # TODO fix
        res = ""
        # res += f"Output dir: {self.output_dir}\n"
        # res += f"Config: {str(self.config)}\n"
        # res += "\n\n=====Stats=====\n"
        # res += f"Number of jiras: {self.umbrella_data.no_of_jiras}\n"
        # res += f"Number of files changed: {self.umbrella_data.no_of_files}\n"
        # for commits_by_branch in self.umbrella_data.upstream_commits_by_branch.values():
        #     res += f"Number of commits on branch {commits_by_branch.branch}: {commits_by_branch.no_of_commits}\n"
        return res
