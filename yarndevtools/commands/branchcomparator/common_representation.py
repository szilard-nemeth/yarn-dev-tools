import os
from abc import ABC, abstractmethod
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
)
from pythoncommons.string_utils import StringUtils

from yarndevtools.commands.branchcomparator.common import BranchType, BranchData, MatchingResultBase, CommonUtils
from yarndevtools.commands_common import CommitData
from yarndevtools.common.shared_command_utils import HtmlHelper
from yarndevtools.constants import SummaryFile

LOG = LoggerFactory.get_logger(__name__)


class BranchComparatorHeader(Enum):
    COMMIT_DATE = "Commit date"
    COMMIT_MSG = "Commit message"
    JIRA_ID = "Jira ID"
    ROW = "Row"
    FILE = "File"
    NO_OF_LINES = "# of lines"
    COMMITTER = "Committer"


class BranchComparatorTable(GenericTableWithHeader):
    def __init__(
        self,
        header_title,
        header: List[str],
        source_data: Any,
        rendered_table: str,
        table_fmt: TabulateTableFormat,
        colorized: bool = False,
        branch: str = None,
    ):
        super().__init__(header_title, header, source_data, rendered_table, table_fmt=table_fmt, colorized=colorized)
        self.branch = branch

    @property
    def is_branch_based(self):
        return self.branch is not None


class BranchComparatorTableStyle(Enum):
    REGULAR = "regular"
    REGULAR_WITH_COLORS = "regular_colorized"
    BRANCH_BASED = "branch_based"


class BranchComparatorTableType(Enum):
    # For simple algorithm
    RESULT_FILES = ("result_files", "RESULT FILES", BranchComparatorTableStyle.REGULAR)
    UNIQUE_ON_BRANCH = ("unique_on_branch", "UNIQUE ON BRANCH $$", BranchComparatorTableStyle.BRANCH_BASED)
    COMMON_COMMITS_SINCE_DIVERGENCE = (
        "common_commits_since_divergence",
        "COMMON COMMITS SINCE BRANCHES DIVERGED",
        BranchComparatorTableStyle.REGULAR,
    )
    ALL_COMMITS_MERGED = (
        "all_commits_merged",
        "ALL COMMITS (MERGED LIST)",
        BranchComparatorTableStyle.REGULAR_WITH_COLORS,
    )

    # For grouped algorithm
    MATCHED_COMMIT_GROUPS = ("matched_commit_groups", "MATCHED COMMIT GROUPS", BranchComparatorTableStyle.REGULAR)
    UNMATCHED_COMMIT_GROUPS = (
        "unmatched_commit_groups",
        "UNMATCHED COMMIT GROUPS ON BRANCH $$",
        BranchComparatorTableStyle.BRANCH_BASED,
    )

    def __init__(self, key, header_value, table_type):
        self.key = key
        self.header = header_value
        self.table_type = table_type


class SummaryDataAbs(ABC):
    def __init__(self, config, branches, matching_result):
        self.config = config
        self.output_dir: str = config.output_dir
        self.branches = branches
        self.branch_data: Dict[BranchType, BranchData] = branches.branch_data
        self.maching_result = matching_result

    def add_stats_no_of_commits_branch(self, res):
        res += "\n\n=====Stats: BRANCHES=====\n"
        for br_type, br_data in self.branch_data.items():
            res += f"Number of commits on {br_type.value} '{br_data.name}': {br_data.number_of_commits}\n"
        return res

    def add_stats_unique_commits_legacy_script(self, res):
        if self.config.run_legacy_script:
            res += "\n\n=====Stats: UNIQUE COMMITS [LEGACY SCRIPT]=====\n"
            for br_type, br_data in self.branch_data.items():
                res += f"Number of unique commits on {br_type.value} '{br_data.name}': {len(br_data.unique_jira_ids_legacy_script)}\n"
        else:
            res += "\n\n=====Stats: UNIQUE COMMITS [LEGACY SCRIPT] - EXECUTION SKIPPED, NO DATA =====\n"
        return res

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
                f"{len(br_data.commits_after_merge_base_filtered)}\n"
            )
        return res


class RenderedSummaryAbs(ABC):
    """
    Properties of tables: Table format, RenderedTableType, Branch, Colorized or not.
    - Table format: Normal (regular) / HTML / Any future formats.
    - RenderedTableType: all values of enum.
    - Branch: Some tables are branch-based, e.g. RenderedTableType.UNIQUE_ON_BRANCH
    - Colorized: Bool value indicating if the table values are colorized
    """

    def __init__(self, summary_data, matching_result, valid_tables: List[BranchComparatorTableType]):
        self.summary_data = summary_data
        self.matching_result = matching_result
        self.table_order: List[BranchComparatorTableType] = valid_tables
        self._tables: Dict[BranchComparatorTableType, List[BranchComparatorTable]] = {}
        self._tables_with_branch: Dict[BranchComparatorTableType, bool] = {
            BranchComparatorTableType.UNIQUE_ON_BRANCH: True,
            BranchComparatorTableType.UNMATCHED_COMMIT_GROUPS: True,
        }

    def add_result_files_table(self):
        result_files_data = sorted(
            FileUtils.find_files(self.summary_data.output_dir, regex=".*", full_path_result=True)
        )
        table_type = BranchComparatorTableType.RESULT_FILES
        h = BranchComparatorHeader
        header = [h.ROW.value, h.FILE.value, h.NO_OF_LINES.value]

        render_conf = TableRenderingConfig(
            row_callback=lambda file: (file, len(FileUtils.read_file(file).splitlines())),
            print_result=False,
            max_width=200,
            max_width_separator=os.sep,
            tabulate_formats=DEFAULT_TABLE_FORMATS,
        )
        gen_tables = ResultPrinter.print_tables(
            data=result_files_data,
            header=header,
            render_conf=render_conf,
        )

        for table_fmt, table in gen_tables.items():
            self.add_table(
                table_type,
                BranchComparatorTable(
                    table_type.header,
                    header,
                    result_files_data,
                    table,
                    table_fmt=table_fmt,
                    colorized=False,
                    branch=None,
                ),
            )

    def add_unique_commit_tables(self, matching_result):
        table_type = BranchComparatorTableType.UNIQUE_ON_BRANCH
        for br_type, br_data in self.summary_data.branch_data.items():
            header_value = table_type.header.replace("$$", br_data.name)
            h = BranchComparatorHeader
            header = [h.ROW.value, h.JIRA_ID.value, h.COMMIT_MSG.value, h.COMMIT_DATE.value, h.COMMITTER.value]
            source_data = matching_result.unique_commits[br_type]

            render_conf = TableRenderingConfig(
                row_callback=lambda commit: (commit.jira_id, commit.message, commit.date, commit.committer),
                print_result=False,
                max_width=80,
                max_width_separator=" ",
                tabulate_formats=DEFAULT_TABLE_FORMATS,
            )
            gen_tables = ResultPrinter.print_tables(data=source_data, header=header, render_conf=render_conf)
            for table_fmt, table in gen_tables.items():
                self.add_table(
                    table_type,
                    BranchComparatorTable(
                        header_value,
                        header,
                        source_data,
                        table,
                        table_fmt=table_fmt,
                        colorized=False,
                        branch=br_data.name,
                    ),
                )

    def add_table(self, ttype: BranchComparatorTableType, table: BranchComparatorTable):
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
        ttype: BranchComparatorTableType,
        colorized: bool = False,
        table_fmt: TabulateTableFormat = TabulateTableFormat.GRID,
        branch: str = None,
    ):
        tables = self._tables[ttype]
        return list(
            filter(lambda t: t.colorized == colorized and t.table_fmt == table_fmt and t.branch == branch, tables)
        )

    def get_branch_based_tables(self, ttype: BranchComparatorTableType, table_fmt: TabulateTableFormat):
        tables: List[BranchComparatorTable] = []
        for br_type, br_data in self.summary_data.branch_data.items():
            tables.extend(self.get_tables(ttype, colorized=False, table_fmt=table_fmt, branch=br_data.name))
        return tables

    def generate_summary_string(self):
        a_normal_table = self.get_tables(list(self.table_order)[0], table_fmt=TabulateTableFormat.GRID)[0]
        length_of_table_first_line = StringUtils.get_first_line_of_multiline_str(a_normal_table.table)
        summary_str = "\n\n" + (
            StringUtils.generate_header_line("SUMMARY", char="═", length=len(length_of_table_first_line)) + "\n"
        )
        summary_str += str(self.summary_data) + "\n\n"
        return summary_str

    def generate_summary_msgs(self):
        self.summary_str = self.generate_summary_string()

        def regular_table(table_type: BranchComparatorTableType):
            return self.get_tables(table_type, colorized=False, table_fmt=TabulateTableFormat.GRID, branch=None)

        def branch_table(table_type: BranchComparatorTableType):
            return self.get_branch_based_tables(table_type, table_fmt=TabulateTableFormat.GRID)

        def html_table(table_type: BranchComparatorTableType):
            return self.get_tables(table_type, colorized=False, table_fmt=TabulateTableFormat.HTML, branch=None)

        def html_branch_table(table_type: BranchComparatorTableType):
            return self.get_branch_based_tables(table_type, table_fmt=TabulateTableFormat.HTML)

        def regular_colorized_table(table_type: BranchComparatorTableType, colorized=False):
            return self.get_tables(table_type, table_fmt=TabulateTableFormat.GRID, colorized=colorized, branch=None)

        printable_tables: List[BranchComparatorTable] = []
        writable_tables: List[BranchComparatorTable] = []
        html_tables: List[BranchComparatorTable] = []
        for rtt in self.table_order:
            if rtt.table_type == BranchComparatorTableStyle.REGULAR:
                printable_tables.extend(regular_table(rtt))
                writable_tables.extend(regular_table(rtt))
                html_tables.extend(html_table(rtt))
            elif rtt.table_type == BranchComparatorTableStyle.REGULAR_WITH_COLORS:
                printable_tables.extend(regular_colorized_table(rtt, colorized=True))
                writable_tables.extend(regular_colorized_table(rtt, colorized=False))
                html_tables.extend(html_table(rtt))
            elif rtt.table_type == BranchComparatorTableStyle.BRANCH_BASED:
                printable_tables.extend(branch_table(rtt))
                writable_tables.extend(branch_table(rtt))
                html_tables.extend(html_branch_table(rtt))

        return (
            HtmlHelper.generate_summary_str(printable_tables, self.summary_str),
            HtmlHelper.generate_summary_str(writable_tables, self.summary_str),
            HtmlHelper.generate_summary_html(html_tables, self.summary_str),
        )


class OutputManagerAbs(ABC):
    LINE_SEPARATOR = "=" * 80

    def __init__(self, config, branch_names: Dict[BranchType, str]):
        self.config = config
        self.branch_names: Dict[BranchType, str] = branch_names

    @abstractmethod
    def write_commit_match_result_files(
        self, branch_data: Dict[BranchType, BranchData], matching_result: MatchingResultBase
    ):
        pass

    def print_or_write_to_file_before_compare(
        self, branch_data: Dict[BranchType, BranchData], merge_base: CommitData, matching_result: MatchingResultBase
    ):
        LOG.info(f"Merge base of branches: {merge_base}")
        feature_br: BranchData = branch_data[BranchType.FEATURE]
        master_br: BranchData = branch_data[BranchType.MASTER]
        LOG.info(
            f"Detected {len(matching_result.before_merge_base)} common commits before merge-base between "
            f"'{feature_br.name}' and '{master_br.name}'"
        )

        for br_type, br_data in branch_data.items():
            LOG.info(f"Printing jira IDs for {br_data.type.value}...")
            for c in br_data.commits_after_merge_base:
                LOG.info(f"Jira ID: {c.jira_id}, commit message: {c.message}")
            if self.config.console_mode:
                LOG.info(f"Found {br_data.number_of_commits} commits on {br_type.value}: {br_data.name}")
            if self.config.save_to_file:
                # We would like to maintain descending order of commits in printouts
                self.write_to_file_or_console("git log output full raw", br_data, list(reversed(br_data.commit_objs)))

            self.write_to_file_or_console("before mergebase commits", br_data, br_data.commits_before_merge_base)
            self.write_to_file_or_console("after mergebase commits", br_data, br_data.commits_after_merge_base)

            LOG.combined_log(
                "Found all commits with missing Jira ID:",
                info_coll=branch_data[br_type].all_commits_with_missing_jira_id,
                debug_coll=branch_data[br_type].all_commits_with_missing_jira_id,
                debug_coll_func=StringUtils.list_to_multiline_string,
            )

            LOG.combined_log(
                f"Found {br_data.type.value} commits after merge-base with missing Jira ID: ",
                coll=br_data.commits_with_missing_jira_id,
                debug_coll_func=StringUtils.list_to_multiline_string,
            )
            filtered_commit_list: List[CommitData] = br_data.filtered_commit_list
            LOG.combined_log(
                f"Found {br_data.type.value} commits after merge-base with missing Jira ID "
                f"(after applied author filter: {self.config.commit_author_exceptions}): ",
                coll=filtered_commit_list,
                debug_coll_func=StringUtils.list_to_multiline_string,
            )

            self.write_to_file_or_console("commits missing jira id", br_data, br_data.commits_with_missing_jira_id)
            self.write_to_file_or_console("commits missing jira id filtered", br_data, filtered_commit_list)

    def write_to_file_or_console(self, output_type: str, branch: BranchData, commits: List[CommitData]):
        contents = CommonUtils.convert_commits_to_oneline_strings(commits)
        self._write_to_file_or_console_branch_data(branch, contents, output_type)

    def _write_to_file_or_console_branch_data(self, branch: BranchData, contents, output_type):
        if self.config.console_mode:
            LOG.info(f"Printing {output_type} for branch {branch.type.name}: {contents}")
        else:
            fn_prefix = self._convert_output_type_str_to_file_prefix(output_type)
            f = self._generate_filename(self.config.output_dir, fn_prefix, branch.shortname)
            LOG.info(f"Saving {output_type} for branch {branch.type.name} to file: {f}")
            FileUtils.save_to_file(f, contents)

    def _write_to_file_or_console(self, contents, output_type, add_sep_to_end=False):
        if self.config.console_mode:
            LOG.info(f"Printing {output_type}: {contents}")
        else:
            fn_prefix = self._convert_output_type_str_to_file_prefix(output_type, add_sep_to_end=add_sep_to_end)
            f = self._generate_filename(self.config.output_dir, fn_prefix)
            LOG.info(f"Saving {output_type} to file: {f}")
            FileUtils.save_to_file(f, contents)

    def print_and_save_summary(self, rendered_summary):
        LOG.info(rendered_summary.printable_summary_str)

        filename = FileUtils.join_path(self.config.output_dir, SummaryFile.TXT.value)
        LOG.info(f"Saving summary to text file: {filename}")
        FileUtils.save_to_file(filename, rendered_summary.writable_summary_str)

        filename = FileUtils.join_path(self.config.output_dir, SummaryFile.HTML.value)
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
