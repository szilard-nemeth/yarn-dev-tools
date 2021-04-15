import os
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Dict, Any

from bs4 import BeautifulSoup
from pythoncommons.file_utils import FileUtils
from pythoncommons.result_printer import (
    TabulateTableFormat,
    ResultPrinter,
    DEFAULT_TABLE_FORMATS,
    BoolConversionConfig,
    ColorizeConfig,
    MatchType,
    Color,
    ColorDescriptor,
    EvaluationMethod,
)
from pythoncommons.string_utils import StringUtils

from yarndevtools.commands.branchcomparator.common import BranchType, BranchData
from yarndevtools.commands_common import CommitData

HEADER_COMMIT_DATE = "Commit date"
HEADER_COMMIT_MSG = "Commit message"
HEADER_JIRA_ID = "Jira ID"
HEADER_ROW = "Row"
HEADER_FILE = "File"
HEADER_NO_OF_LINES = "# of lines"
HEADER_COMMITTER = "Committer"


class SummaryDataAbs(ABC):
    def __init__(self, conf, branches: Any):
        self.output_dir: str = conf.output_dir
        self.run_legacy_script: bool = conf.run_legacy_script

        # Dict-based data structure, key: BranchType
        # These are set before comparing the branches
        self.branches = branches
        self.branch_data: Dict[BranchType, BranchData] = branches.branch_data

    @abstractmethod
    def common_commits_after_merge_base(self):
        pass

    @property
    def all_commits(self):
        all_commits: List[CommitData] = (
            []
            + self.branch_data[BranchType.MASTER].unique_commits
            + self.branch_data[BranchType.FEATURE].unique_commits
            + self.common_commits_after_merge_base()
        )
        all_commits.sort(key=lambda c: c.date, reverse=True)
        return all_commits

    @property
    def all_commits_presence_matrix(self) -> List[List]:
        rows: List[List] = []
        for commit in self.all_commits:
            jira_id = commit.jira_id
            row: List[Any] = [jira_id, commit.message, commit.date, commit.committer]

            presence: List[bool] = []
            if self.is_jira_id_present_on_branch(jira_id, BranchType.MASTER) and self.is_jira_id_present_on_branch(
                jira_id, BranchType.FEATURE
            ):
                presence = [True, True]
            elif self.is_jira_id_present_on_branch(jira_id, BranchType.MASTER):
                presence = [True, False]
            elif self.is_jira_id_present_on_branch(jira_id, BranchType.FEATURE):
                presence = [False, True]
            row.extend(presence)
            rows.append(row)
        return rows

    def get_branch_names(self):
        return [bd.name for bd in self.branch_data.values()]

    def get_branch(self, br_type: BranchType):
        return self.branch_data[br_type]

    def is_jira_id_present_on_branch(self, jira_id: str, br_type: BranchType):
        br: BranchData = self.get_branch(br_type)
        return jira_id in br.jira_id_to_commits

    def __str__(self):
        res = ""
        res += f"Output dir: {self.output_dir}\n"
        res = self.add_stats_no_of_commits_branch(res)
        res = self.add_stats_no_of_unique_commits_on_branch(res)
        res = self.add_stats_unique_commits_legacy_script(res)
        res = self.add_stats_common_commits_on_branches(res)
        res = self.add_stats_commits_with_missing_jira_id(res)
        res = self.add_stats_common_commit_details(res)
        return res

    @abstractmethod
    def add_stats_common_commit_details(self, res):
        pass

    def add_stats_commits_with_missing_jira_id(self, res):
        for br_type, br_data in self.branch_data.items():
            res += f"\n\n=====Stats: COMMITS WITH MISSING JIRA ID ON BRANCH: {br_data.name}=====\n"
            res += f"Number of all commits with missing Jira ID: {len(self.branches.all_commits_with_missing_jira_id[br_type])}\n"
            res += (
                f"Number of commits with missing Jira ID after merge-base: "
                f"{len(br_data.commits_with_missing_jira_id)}\n"
            )
            res += (
                f"Number of commits with missing Jira ID after merge-base, filtered by author exceptions: "
                f"{len(br_data.commits_with_missing_jira_id_filtered)}\n"
            )
        return res

    @abstractmethod
    def add_stats_common_commits_on_branches(self, res):
        pass

    def add_stats_unique_commits_legacy_script(self, res):
        if self.run_legacy_script:
            res += "\n\n=====Stats: UNIQUE COMMITS [LEGACY SCRIPT]=====\n"
            for br_type, br_data in self.branch_data.items():
                res += f"Number of unique commits on {br_type.value} '{br_data.name}': {len(br_data.unique_jira_ids_legacy_script)}\n"
        else:
            res += "\n\n=====Stats: UNIQUE COMMITS [LEGACY SCRIPT] - EXECUTION SKIPPED, NO DATA =====\n"
        return res

    def add_stats_no_of_unique_commits_on_branch(self, res):
        res += "\n\n=====Stats: UNIQUE COMMITS=====\n"
        for br_type, br_data in self.branch_data.items():
            res += f"Number of unique commits on {br_type.value} '{br_data.name}': {len(br_data.unique_commits)}\n"
        return res

    def add_stats_no_of_commits_branch(self, res):
        res += "\n\n=====Stats: BRANCHES=====\n"
        for br_type, br_data in self.branch_data.items():
            res += f"Number of commits on {br_type.value} '{br_data.name}': {br_data.number_of_commits}\n"
        return res


class TableWithHeader:
    def __init__(
        self, header_title, table: str, table_fmt: TabulateTableFormat, colorized: bool = False, branch: str = None
    ):
        self.header = (
            StringUtils.generate_header_line(
                header_title, char="═", length=len(StringUtils.get_first_line_of_multiline_str(table))
            )
            + "\n"
        )
        self.table = table
        self.table_fmt: TabulateTableFormat = table_fmt
        self.colorized = colorized
        self.branch = branch

    @property
    def is_branch_based(self):
        return self.branch is not None

    def __str__(self):
        return self.header + self.table


class RenderedTableType(Enum):
    RESULT_FILES = ("result_files", "RESULT FILES")
    UNIQUE_ON_BRANCH = ("unique_on_branch", "UNIQUE ON BRANCH $$")
    COMMON_COMMITS_SINCE_DIVERGENCE = ("common_commits_since_divergence", "COMMON COMMITS SINCE BRANCHES DIVERGED")
    ALL_COMMITS_MERGED = ("all_commits_merged", "ALL COMMITS (MERGED LIST)")

    def __init__(self, key, header_value):
        self.key = key
        self.header = header_value


class RenderedSummary:
    """
    Properties of tables: Table format, RenderedTableType, Branch, Colorized or not.
    - Table format: Normal (regular) / HTML / Any future formats.
    - RenderedTableType: all values of enum.
    - Branch: Some tables are branch-based, e.g. RenderedTableType.UNIQUE_ON_BRANCH
    - Colorized: Bool value indicating if the table values are colorized
    """

    def __init__(self, summary_data: SummaryDataAbs):
        self._tables: Dict[RenderedTableType, List[TableWithHeader]] = {}
        self._tables_with_branch: Dict[RenderedTableType, bool] = {RenderedTableType.UNIQUE_ON_BRANCH: True}
        self.summary_data = summary_data
        self.add_result_files_table()
        self.add_unique_commit_tables()
        self.add_common_commits_table()
        self.add_all_commits_tables()
        self.summary_str = self.generate_summary_string()
        self.printable_summary_str, self.writable_summary_str, self.html_summary = self.generate_summary_msgs()

    def add_table(self, ttype: RenderedTableType, table: TableWithHeader):
        if table.is_branch_based and ttype not in self._tables_with_branch:
            raise ValueError(
                f"Unexpected table type for branch-based table: {ttype}. "
                f"Possible table types with branch info: {[k.name for k in self._tables_with_branch.keys()]}"
            )
        if ttype not in self._tables:
            self._tables[ttype] = []
        self._tables[ttype].append(table)

    def get_tables(
        self,
        ttype: RenderedTableType,
        colorized: bool = False,
        table_fmt: TabulateTableFormat = TabulateTableFormat.GRID,
        branch: str = None,
    ):
        tables = self._tables[ttype]
        result: List[TableWithHeader] = []
        for table in tables:
            # TODO simplify with filter
            if table.colorized == colorized and table.table_fmt == table_fmt and table.branch == branch:
                result.append(table)
        return result

    def get_branch_based_tables(self, ttype: RenderedTableType, table_fmt: TabulateTableFormat):
        tables: List[TableWithHeader] = []
        for br_type, br_data in self.summary_data.branch_data.items():
            tables.extend(self.get_tables(ttype, colorized=False, table_fmt=table_fmt, branch=br_data.name))
        return tables

    @staticmethod
    def from_summary_data(summary_data: SummaryDataAbs):
        return RenderedSummary(summary_data)

    def add_result_files_table(self):
        result_files_data = sorted(
            FileUtils.find_files(self.summary_data.output_dir, regex=".*", full_path_result=True)
        )
        table_type = RenderedTableType.RESULT_FILES
        gen_tables = ResultPrinter.print_tables(
            result_files_data,
            lambda file: (file, len(FileUtils.read_file(file).splitlines())),
            header=[HEADER_ROW, HEADER_FILE, HEADER_NO_OF_LINES],
            print_result=False,
            max_width=200,
            max_width_separator=os.sep,
            tabulate_fmts=DEFAULT_TABLE_FORMATS,
        )

        for table_fmt, table in gen_tables.items():
            self.add_table(
                table_type, TableWithHeader(table_type.header, table, table_fmt=table_fmt, colorized=False, branch=None)
            )

    def add_unique_commit_tables(self):
        table_type = RenderedTableType.UNIQUE_ON_BRANCH
        for br_type, br_data in self.summary_data.branch_data.items():
            header_value = table_type.header.replace("$$", br_data.name)
            gen_tables = ResultPrinter.print_tables(
                br_data.unique_commits,
                lambda commit: (commit.jira_id, commit.message, commit.date, commit.committer),
                header=[HEADER_ROW, HEADER_JIRA_ID, HEADER_COMMIT_MSG, HEADER_COMMIT_DATE, HEADER_COMMITTER],
                print_result=False,
                max_width=80,
                max_width_separator=" ",
                tabulate_fmts=DEFAULT_TABLE_FORMATS,
            )
            for table_fmt, table in gen_tables.items():
                self.add_table(
                    table_type,
                    TableWithHeader(header_value, table, table_fmt=table_fmt, colorized=False, branch=br_data.name),
                )

    def add_common_commits_table(self):
        table_type = RenderedTableType.COMMON_COMMITS_SINCE_DIVERGENCE
        gen_tables = ResultPrinter.print_tables(
            self.summary_data.common_commits_after_merge_base(),
            lambda commit: (commit.jira_id, commit.message, commit.date, commit.committer),
            header=[HEADER_ROW, HEADER_JIRA_ID, HEADER_COMMIT_MSG, HEADER_COMMIT_DATE, HEADER_COMMITTER],
            print_result=False,
            max_width=80,
            max_width_separator=" ",
            tabulate_fmts=DEFAULT_TABLE_FORMATS,
        )
        for table_fmt, table in gen_tables.items():
            self.add_table(table_type, TableWithHeader(table_type.header, table, table_fmt=table_fmt, colorized=False))

    def add_all_commits_tables(self):
        all_commits: List[List] = self.summary_data.all_commits_presence_matrix

        header = [HEADER_ROW, HEADER_JIRA_ID, HEADER_COMMIT_MSG, HEADER_COMMIT_DATE, HEADER_COMMITTER]
        header.extend(self.summary_data.get_branch_names())

        # Adding 1 because row id will be added as first column
        row_len = len(all_commits[0]) + 1
        color_conf = ColorizeConfig(
            [
                ColorDescriptor(bool, True, Color.GREEN, MatchType.ALL, (0, row_len), (0, row_len)),
                ColorDescriptor(bool, False, Color.RED, MatchType.ANY, (0, row_len), (0, row_len)),
            ],
            eval_method=EvaluationMethod.ALL,
        )
        self._add_all_comits_table(header, all_commits, colorize_conf=color_conf)
        self._add_all_comits_table(header, all_commits, colorize_conf=None)

    def _add_all_comits_table(self, header, all_commits, colorize_conf: ColorizeConfig = None):
        table_type = RenderedTableType.ALL_COMMITS_MERGED
        colorize = True if colorize_conf else False
        gen_tables = ResultPrinter.print_tables(
            all_commits,
            lambda row: row,
            header=header,
            print_result=False,
            max_width=100,
            max_width_separator=" ",
            bool_conversion_config=BoolConversionConfig(),
            colorize_config=colorize_conf,
        )

        for table_fmt, table in gen_tables.items():
            self.add_table(
                table_type, TableWithHeader(table_type.header, table, table_fmt=table_fmt, colorized=colorize)
            )

    def generate_summary_string(self):
        a_normal_table = self.get_tables(
            RenderedTableType.COMMON_COMMITS_SINCE_DIVERGENCE, table_fmt=TabulateTableFormat.GRID
        )[0]
        length_of_table_first_line = StringUtils.get_first_line_of_multiline_str(a_normal_table.table)
        summary_str = "\n\n" + (
            StringUtils.generate_header_line("SUMMARY", char="═", length=len(length_of_table_first_line)) + "\n"
        )
        summary_str += str(self.summary_data) + "\n\n"
        return summary_str

    def generate_summary_msgs(self):
        def regular_table(table_type: RenderedTableType):
            return self.get_tables(table_type, colorized=False, table_fmt=TabulateTableFormat.GRID, branch=None)

        def get_branch_tables(table_type: RenderedTableType):
            return self.get_branch_based_tables(table_type, table_fmt=TabulateTableFormat.GRID)

        def html_table(table_type: RenderedTableType):
            return self.get_tables(table_type, colorized=False, table_fmt=TabulateTableFormat.HTML, branch=None)

        def get_html_branch_tables(table_type: RenderedTableType):
            return self.get_branch_based_tables(table_type, table_fmt=TabulateTableFormat.HTML)

        def get_colorized_tables(table_type: RenderedTableType, colorized=False):
            return self.get_tables(table_type, table_fmt=TabulateTableFormat.GRID, colorized=colorized, branch=None)

        rt = regular_table
        ht = html_table
        rtt = RenderedTableType
        bt = get_branch_tables
        hbt = get_html_branch_tables
        cbt = get_colorized_tables

        printable_tables: List[TableWithHeader] = (
            rt(rtt.RESULT_FILES)
            + bt(rtt.UNIQUE_ON_BRANCH)
            + rt(rtt.COMMON_COMMITS_SINCE_DIVERGENCE)
            + cbt(rtt.ALL_COMMITS_MERGED, colorized=True)
        )
        writable_tables: List[TableWithHeader] = (
            rt(rtt.RESULT_FILES)
            + bt(rtt.UNIQUE_ON_BRANCH)
            + rt(rtt.COMMON_COMMITS_SINCE_DIVERGENCE)
            + cbt(rtt.ALL_COMMITS_MERGED, colorized=False)
        )

        html_tables: List[TableWithHeader] = (
            ht(rtt.RESULT_FILES)
            + hbt(rtt.UNIQUE_ON_BRANCH)
            + ht(rtt.COMMON_COMMITS_SINCE_DIVERGENCE)
            + ht(rtt.ALL_COMMITS_MERGED)
        )
        return (
            self._generate_summary_str(printable_tables),
            self._generate_summary_str(writable_tables),
            self.generate_summary_html(html_tables),
        )

    def _generate_summary_str(self, tables):
        printable_summary_str: str = self.summary_str
        for table in tables:
            printable_summary_str += str(table)
            printable_summary_str += "\n\n"
        return printable_summary_str

    def generate_summary_html(self, html_tables, separator_tag="hr", add_breaks=2) -> str:
        html_sep = f"<{separator_tag}/>"
        html_sep += add_breaks * "<br/>"
        tables_html: str = html_sep
        tables_html += html_sep.join([f"<h1>{h.header}</h1>" + h.table for h in html_tables])

        soup = BeautifulSoup()
        self._add_summary_as_html_paragraphs(soup)
        html = soup.new_tag("html")
        self._add_html_head_and_style(soup, html)
        html.append(BeautifulSoup(tables_html, "html.parser"))
        soup.append(html)
        return soup.prettify()

    def _add_summary_as_html_paragraphs(self, soup):
        lines = self.summary_str.splitlines()
        for line in lines:
            p = soup.new_tag("p")
            p.append(line)
            soup.append(p)

    def _add_html_head_and_style(self, soup, html):
        head = soup.new_tag("head")
        style = soup.new_tag("style")
        style.string = """
table, th, td {
  border: 1px solid black;
}
"""
        head.append(style)
        html.append(head)
