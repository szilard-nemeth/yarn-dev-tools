import os
from enum import Enum
from typing import Dict, List, Tuple

from bs4 import BeautifulSoup
from git import Commit
from pythoncommons.date_utils import DateUtils
from pythoncommons.file_utils import FileUtils
from pythoncommons.logging_utils import LoggerFactory
from pythoncommons.result_printer import (
    ResultPrinter,
    ColorizeConfig,
    ColorDescriptor,
    Color,
    MatchType,
    EvaluationMethod,
    BoolConversionConfig,
    TabulateTableFormat,
    DEFAULT_TABLE_FORMATS,
)
from pythoncommons.string_utils import StringUtils

from yarndevtools.commands.branchcomparator.common import BranchType, BranchData
from yarndevtools.commands.branchcomparator.common_representation import SummaryDataAbs
from yarndevtools.commands.branchcomparator.legacy_script import LegacyScriptRunner
from yarndevtools.commands.branchcomparator.simple_matching import SimpleCommitMatcher, CommonCommits
from yarndevtools.commands_common import (
    CommitData,
    GitLogLineFormat,
    GitLogParseConfig,
    MatchAllJiraIdStrategy,
    JiraIdTypePreference,
    JiraIdChoosePreference,
)
from yarndevtools.constants import ANY_JIRA_ID_PATTERN, REPO_ROOT_DIRNAME, SUMMARY_FILE_TXT, SUMMARY_FILE_HTML
from pythoncommons.git_wrapper import GitWrapper

HEADER_COMMIT_DATE = "Commit date"
HEADER_COMMIT_MSG = "Commit message"
HEADER_JIRA_ID = "Jira ID"
HEADER_ROW = "Row"
HEADER_FILE = "File"
HEADER_NO_OF_LINES = "# of lines"
HEADER_COMMITTER = "Committer"

LOG = LoggerFactory.get_logger(__name__)


class BranchComparatorConfig:
    def __init__(self, output_dir: str, args):
        self.output_dir = FileUtils.ensure_dir_created(
            FileUtils.join_path(output_dir, f"session-{DateUtils.now_formatted('%Y%m%d_%H%M%S')}")
        )
        self.commit_author_exceptions = args.commit_author_exceptions
        self.console_mode = True if "console_mode" in args and args.console_mode else False
        self.save_to_file = not self.console_mode
        self.fail_on_missing_jira_id = False
        self.run_legacy_script = args.run_legacy_script
        self.legacy_compare_script_path = BranchComparatorConfig.find_git_compare_script()

    @staticmethod
    def find_git_compare_script():
        repo_root_dir = FileUtils.find_repo_root_dir(__file__, REPO_ROOT_DIRNAME)
        return FileUtils.join_path(repo_root_dir, "legacy-scripts", "branch-comparator", "git_compare.sh")


class Branches:
    def __init__(self, conf: BranchComparatorConfig, repo: GitWrapper, branch_dict: Dict[BranchType, str]):
        self.config = conf
        self.repo = repo
        self.branch_data: Dict[BranchType, BranchData] = {}
        for br_type in BranchType:
            branch_name = branch_dict[br_type]
            self.branch_data[br_type] = BranchData(br_type, branch_name)

        # TODO make this object instance configurable
        self.commit_matcher = SimpleCommitMatcher(self.branch_data)
        self.summary: SummaryDataAbs = self.commit_matcher.create_summary_data(self.config, self)

        # These are set later
        self.merge_base: CommitData or None = None

    def get_branch(self, br_type: BranchType) -> BranchData:
        return self.branch_data[br_type]

    @staticmethod
    def _generate_filename(basedir, prefix, branch_name="") -> str:
        return FileUtils.join_path(basedir, f"{prefix}{StringUtils.replace_special_chars(branch_name)}")

    def validate(self, br_type: BranchType):
        br_data = self.branch_data[br_type]
        branch_exist = self.repo.is_branch_exist(br_data.name)
        if not branch_exist:
            LOG.error(f"{br_data.type.name} does not exist with name '{br_data.name}'")
        return branch_exist

    def execute_git_log(self):
        for br_type in BranchType:
            branch: BranchData = self.branch_data[br_type]
            branch.gitlog_results = self.repo.log(branch.name, oneline_with_date_author_committer=True)
            parse_config = GitLogParseConfig(
                log_format=GitLogLineFormat.ONELINE_WITH_DATE_AUTHOR_COMMITTER,
                pattern=ANY_JIRA_ID_PATTERN,
                allow_unmatched_jira_id=True,
                print_unique_jira_projects=True,
                jira_id_parse_strategy=MatchAllJiraIdStrategy(
                    type_preference=JiraIdTypePreference.UPSTREAM,
                    choose_preference=JiraIdChoosePreference.FIRST,
                    fallback_type=JiraIdTypePreference.DOWNSTREAM,
                ),
                keep_parser_state=True,
            )

            # Store commit objects in reverse order (ascending by date)
            branch.set_commit_objs(list(reversed(CommitData.from_git_log_output(branch.gitlog_results, parse_config))))
            for idx, commit in enumerate(branch.commit_objs):
                branch.hash_to_index[commit.hash] = idx
                # TODO add a special key like "NO_JIRA_ID" that groups all commits without jira id, right now these are overwriting
                #  because key will be None
                if commit.jira_id not in branch.jira_id_to_commits:
                    branch.jira_id_to_commits[commit.jira_id] = []
                branch.jira_id_to_commits[commit.jira_id].append(commit)
        # This must be executed after branch.hash_to_index is set
        self.get_merge_base()

    def pre_compare(self):
        feature_br: BranchData = self.branch_data[BranchType.FEATURE]
        master_br: BranchData = self.branch_data[BranchType.MASTER]
        branches = [feature_br, master_br]
        self._sanity_check_commits_before_merge_base(feature_br, master_br)
        self._determine_commits_with_missing_jira_id(branches)

    def print_or_write_to_file_before_compare(self, common_commits):
        LOG.info(f"Merge base of branches: {self.merge_base}")
        feature_br: BranchData = self.branch_data[BranchType.FEATURE]
        master_br: BranchData = self.branch_data[BranchType.MASTER]
        LOG.info(
            f"Detected {len(common_commits.before_merge_base)} common commits before merge-base between "
            f"'{feature_br.name}' and '{master_br.name}'"
        )

        # TODO write commits with multiple jira IDs
        # If fail on missing jira id is configured, fail-fast
        if self.config.fail_on_missing_jira_id:
            # TODO fix this prints size of dict keys which is 2 (feature, master)
            raise ValueError(
                f"Found {len(self.summary.all_commits_with_missing_jira_id)} commits with missing Jira ID! "
                f"Halting as configured"
            )

        for br_type, br_data in self.branch_data.items():
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
                info_coll=self.summary.all_commits_with_missing_jira_id[br_data.type],
                debug_coll=self.summary.all_commits_with_missing_jira_id[br_data.type],
                debug_coll_func=StringUtils.list_to_multiline_string,
            )

            LOG.combined_log(
                f"Found {br_data.type.value} commits after merge-base with missing Jira ID: ",
                coll=br_data.commits_with_missing_jira_id,
                debug_coll_func=StringUtils.list_to_multiline_string,
            )
            LOG.combined_log(
                f"Found {br_data.type.value} commits after merge-base with missing Jira ID "
                f"(after applied author filter: {self.config.commit_author_exceptions}): ",
                coll=br_data.commits_with_missing_jira_id_filtered,
                debug_coll_func=StringUtils.list_to_multiline_string,
            )

            self.write_to_file_or_console("commits missing jira id", br_data, br_data.commits_with_missing_jira_id)
            filtered_commit_list = [c for c in br_data.commits_with_missing_jira_id_filtered.values()]
            self.write_to_file_or_console("commits missing jira id filtered", br_data, filtered_commit_list)

    def get_merge_base(self):
        merge_base: List[Commit] = self.repo.merge_base(
            self.branch_data[BranchType.FEATURE].name, self.branch_data[BranchType.MASTER].name
        )
        if len(merge_base) > 1:
            raise ValueError(f"Ambiguous merge base: {merge_base}.")
        elif len(merge_base) == 0:
            raise ValueError("Merge base not found between branches!")
        self.merge_base = CommitData.from_git_log_str(
            self.repo.log(
                merge_base[0].hexsha,
                oneline_with_date_author_committer=True,
            )[0],
            format=GitLogLineFormat.ONELINE_WITH_DATE_AUTHOR_COMMITTER,
            allow_unmatched_jira_id=True,
        )
        self.summary.merge_base = self.merge_base
        for br_type in BranchType:
            branch: BranchData = self.branch_data[br_type]
            branch.set_merge_base(self.merge_base)

    def compare(self):
        # At this point, sanity check verified commits before merge-base,
        # we can set it from any of master / feature branch
        common_commits = self.commit_matcher.create_common_commits_obj()
        common_commits.before_merge_base = self.branch_data[BranchType.MASTER].commits_before_merge_base
        self.print_or_write_to_file_before_compare(common_commits)

        # Start to compare
        self.commit_matcher.match_commits()
        self.summary._common_commits = common_commits
        self._write_commit_match_result_files(common_commits)

    @staticmethod
    def _sanity_check_commits_before_merge_base(feature_br: BranchData, master_br: BranchData):
        if len(master_br.commits_before_merge_base) != len(feature_br.commits_before_merge_base):
            raise ValueError(
                "Number of commits before merge_base does not match. "
                f"Feature branch has: {len(feature_br.commits_before_merge_base)} commits, "
                f"Master branch has: {len(master_br.commits_before_merge_base)} commits"
            )
        # Commit hashes up to the merge-base commit should be the same for both branches
        for idx, commit1 in enumerate(master_br.commits_before_merge_base):
            commit2 = feature_br.commits_before_merge_base[idx]
            if commit1.hash != commit2.hash:
                raise ValueError(
                    f"Commit hash mismatch below merge-base commit.\n"
                    f"Index: {idx}\n"
                    f"Hash of commit on {feature_br.name}: {commit2.hash}\n"
                    f"Hash of commit on {master_br.name}: {commit1.hash}"
                )

    def _determine_commits_with_missing_jira_id(self, branches: List[BranchData]):
        # TODO write commits with multiple jira IDs
        # If fail on missing jira id is configured, fail-fast
        if self.config.fail_on_missing_jira_id:
            # TODO fix this prints size of dict keys which is 2 (feature, master)
            raise ValueError(
                f"Found {len(self.summary.all_commits_with_missing_jira_id)} commits with missing Jira ID! "
                f"Halting as configured"
            )

        for br_data in branches:
            br_data.commits_with_missing_jira_id = list(
                filter(lambda c: not c.jira_id, br_data.commits_after_merge_base)
            )

            # Create a dict of (commit message, CommitData),
            # filtering all the commits that has author from the authors to filter.
            # IMPORTANT Assumption: Commit message is unique for all commits
            br_data.commits_with_missing_jira_id_filtered = dict(
                [
                    (c.message, c)
                    for c in filter(
                        lambda c: c.author not in self.config.commit_author_exceptions,
                        br_data.commits_with_missing_jira_id,
                    )
                ]
            )

    def write_to_file_or_console(self, output_type: str, branch: BranchData, commits: List[CommitData]):
        contents = StringUtils.list_to_multiline_string([self.convert_commit_to_str(c) for c in commits])
        if self.config.console_mode:
            LOG.info(f"Printing {output_type} for branch {branch.type.name}: {contents}")
        else:
            fn_prefix = Branches._convert_output_type_str_to_file_prefix(output_type)
            f = self._generate_filename(self.config.output_dir, fn_prefix, branch.shortname)
            LOG.info(f"Saving {output_type} for branch {branch.type.name} to file: {f}")
            FileUtils.save_to_file(f, contents)

    def write_commit_list_to_file_or_console(
        self,
        output_type: str,
        commit_groups: List[Tuple[CommitData, CommitData]],
        add_sep_to_end=True,
        add_line_break_between_groups=False,
    ):
        if not add_line_break_between_groups:
            commits = [self.convert_commit_to_str(commit) for tup in commit_groups for commit in tup]
            contents = StringUtils.list_to_multiline_string(commits)
        else:
            contents = ""
            for tup in commit_groups:
                commit_strs = [self.convert_commit_to_str(commit) for commit in tup]
                contents += StringUtils.list_to_multiline_string(commit_strs)
                contents += "\n\n"

        if self.config.console_mode:
            LOG.info(f"Printing {output_type}: {contents}")
        else:
            fn_prefix = Branches._convert_output_type_str_to_file_prefix(output_type, add_sep_to_end=add_sep_to_end)
            f = self._generate_filename(self.config.output_dir, fn_prefix)
            LOG.info(f"Saving {output_type} to file: {f}")
            FileUtils.save_to_file(f, contents)

    @staticmethod
    def _convert_output_type_str_to_file_prefix(output_type, add_sep_to_end=True):
        file_prefix: str = output_type.replace(" ", "-")
        if add_sep_to_end:
            file_prefix += "-"
        return file_prefix

    @staticmethod
    def convert_commit_to_str(commit: CommitData):
        return commit.as_oneline_string(incl_date=True, incl_author=False, incl_committer=True)

    def _write_commit_match_result_files(self, common_commits: CommonCommits):
        self.write_commit_list_to_file_or_console(
            "commit message differs",
            common_commits.matched_only_by_jira_id,
            add_sep_to_end=False,
            add_line_break_between_groups=True,
        )

        self.write_commit_list_to_file_or_console(
            "commits matched by message",
            common_commits.matched_only_by_message,
            add_sep_to_end=False,
            add_line_break_between_groups=True,
        )
        for br_data in self.branch_data.values():
            self.write_to_file_or_console("unique commits", br_data, br_data.unique_commits)


# TODO Handle multiple jira ids?? example: "CDPD-10052. HADOOP-16932"
# TODO Consider revert commits?
# TODO Add documentation
# TODO Check in logs: all results for "Jira ID is the same for commits, but commit message differs"


class BranchComparator:
    """"""

    def __init__(self, args, downstream_repo, output_dir: str):
        self.repo = downstream_repo
        self.config = BranchComparatorConfig(output_dir, args)
        self.branches: Branches = Branches(
            self.config, self.repo, {BranchType.FEATURE: args.feature_branch, BranchType.MASTER: args.master_branch}
        )

    def run(self):
        LOG.info(
            "Starting Branch comparator... \n "
            f"Output dir: {self.config.output_dir}\n"
            f"Master branch: {self.branches.get_branch(BranchType.MASTER).name}\n "
            f"Feature branch: {self.branches.get_branch(BranchType.FEATURE).name}\n "
            f"Commit author exceptions: {self.config.commit_author_exceptions}\n "
            f"Console mode: {self.config.console_mode}\n "
            f"Run legacy comparator script: {self.config.run_legacy_script}\n "
        )
        self.validate_branches()
        # TODO Make fetching optional, argparse argument
        # self.repo.fetch(all=True)
        self.compare()
        if self.config.run_legacy_script:
            LegacyScriptRunner.start(self.config, self.branches, self.repo.repo_path)
        self.print_and_save_summary()

    def validate_branches(self):
        both_exist = self.branches.validate(BranchType.FEATURE)
        both_exist &= self.branches.validate(BranchType.MASTER)
        if not both_exist:
            raise ValueError("Both feature and master branch should be an existing branch. Exiting...")

    def compare(self):
        self.branches.execute_git_log()
        self.branches.pre_compare()
        self.branches.compare()

    def print_and_save_summary(self):
        rendered_sum = RenderedSummary.from_summary_data(self.branches.summary)
        LOG.info(rendered_sum.printable_summary_str)

        filename = FileUtils.join_path(self.config.output_dir, SUMMARY_FILE_TXT)
        LOG.info(f"Saving summary to text file: {filename}")
        FileUtils.save_to_file(filename, rendered_sum.writable_summary_str)

        filename = FileUtils.join_path(self.config.output_dir, SUMMARY_FILE_HTML)
        LOG.info(f"Saving summary to html file: {filename}")
        FileUtils.save_to_file(filename, rendered_sum.html_summary)


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
            self.summary_data.common_commits,
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
