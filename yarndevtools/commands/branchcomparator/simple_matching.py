import logging
import typing
from typing import List, Dict, Tuple
from typing import Set, Any

from pythoncommons.result_printer import (
    ResultPrinter,
    DEFAULT_TABLE_FORMATS,
    BoolConversionConfig,
    ColorizeConfig,
    MatchType,
    Color,
    ColorDescriptor,
    EvaluationMethod,
    TableRenderingConfig,
)
from pythoncommons.string_utils import StringUtils

from yarndevtools.commands.branchcomparator.common import BranchType, BranchData, MatchingResultBase, CommonUtils
from yarndevtools.commands.branchcomparator.common import (
    CommitMatchType,
    CommitMatcherBase,
)
from yarndevtools.commands.branchcomparator.common_representation import (
    SummaryDataAbs,
    RenderedTableType,
    RenderedSummaryAbs,
    TableWithHeader,
    HEADER_ROW,
    HEADER_JIRA_ID,
    HEADER_COMMIT_MSG,
    HEADER_COMMIT_DATE,
    HEADER_COMMITTER,
    OutputManagerAbs,
)
from yarndevtools.commands_common import CommitData

LOG = logging.getLogger(__name__)


class SimpleMatchingResult(MatchingResultBase):
    def __init__(self):
        super().__init__()
        self.after_merge_base: List[Tuple[CommitData, CommitData]] = []

        # Commits matched by message with missing Jira ID
        self.matched_only_by_message: List[Tuple[CommitData, CommitData]] = []

        # Commits matched by Jira ID but not by message
        self.matched_only_by_jira_id: List[Tuple[CommitData, CommitData]] = []

        # Commits matched by Jira ID and by message as well
        self.matched_both: List[Tuple[CommitData, CommitData]] = []

        self.unique_commits: Dict[BranchType, List[CommitData]] = {}

    @property
    def commits_after_merge_base(self):
        return [c[0] for c in self.after_merge_base]


class SimpleCommitMatcherSummaryData(SummaryDataAbs):
    def __init__(self, config, branches, matching_result: SimpleMatchingResult):
        super().__init__(config, branches, matching_result)

    def common_commits_after_merge_base(self):
        return self.maching_result.commits_after_merge_base

    @property
    def all_commits(self):
        all_commits: List[CommitData] = (
            []
            + self.maching_result.unique_commits[BranchType.MASTER]
            + self.maching_result.unique_commits[BranchType.FEATURE]
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
            if self.is_commit_present_on_branch(commit, BranchType.MASTER) and self.is_commit_present_on_branch(
                commit, BranchType.FEATURE
            ):
                presence = [True, True]
            elif self.is_commit_present_on_branch(commit, BranchType.MASTER):
                presence = [True, False]
            elif self.is_commit_present_on_branch(commit, BranchType.FEATURE):
                presence = [False, True]
            row.extend(presence)
            rows.append(row)
        return rows

    def get_branch_names(self):
        return [bd.name for bd in self.branch_data.values()]

    def get_branch(self, br_type: BranchType):
        return self.branch_data[br_type]

    def is_commit_present_on_branch(self, commit: CommitData, br_type: BranchType) -> bool:
        br: BranchData = self.get_branch(br_type)
        if commit.jira_id:
            return commit.jira_id in br.jira_id_to_commits
        else:
            return commit.hash in br.hash_to_index

    def __str__(self):
        res = ""
        res += f"Output dir: {self.output_dir}\n"
        res += f"Config: {str(self.config)}\n"
        res = self.add_stats_no_of_commits_branch(res)
        res = self.add_stats_no_of_unique_commits_on_branch(res)
        res = self.add_stats_unique_commits_legacy_script(res)
        res = self.add_stats_matched_commits_on_branches(res)
        res = self.add_stats_commits_with_missing_jira_id(res)
        res = self.add_stats_matched_commit_details(res)
        return res

    def add_stats_no_of_unique_commits_on_branch(self, res):
        res += "\n\n=====Stats: UNIQUE COMMITS=====\n"
        for br_type, br_data in self.branch_data.items():
            res += (
                f"Number of unique commits on {br_type.value} '{br_data.name}': "
                f"{len(self.maching_result.unique_commits[br_type])}\n"
            )
        return res

    def add_stats_matched_commits_on_branches(self, res):
        res += "\n\n=====Stats: COMMON COMMITS=====\n"
        res += f"Merge-base commit: {self.branches.merge_base.as_oneline_string(incl_date=True)}\n"
        res += f"Number of common commits before merge-base: {len(self.maching_result.before_merge_base)}\n"
        res += f"Number of common commits after merge-base: {len(self.maching_result.after_merge_base)}\n"
        return res

    def add_stats_matched_commit_details(self, res):
        res += "\n\n=====Stats: COMMON COMMITS ACROSS BRANCHES=====\n"
        res += (
            f"Number of common commits with missing Jira ID, matched by commit message: "
            f"{len(self.maching_result.matched_only_by_message)}\n"
        )
        res += (
            f"Number of common commits with matching Jira ID but different commit message: "
            f"{len(self.maching_result.matched_only_by_jira_id)}\n"
        )
        res += (
            f"Number of common commits with matching Jira ID and commit message: "
            f"{len(self.maching_result.matched_both)}\n"
        )
        return res


class SimpleCommitMatcher(CommitMatcherBase):
    def __init__(self, branch_data: Dict[BranchType, BranchData]):
        super().__init__(branch_data, SimpleMatchingResult())
        self.matching_result = typing.cast(SimpleMatchingResult, self.matching_result)

    def match_commits(self, config, output_manager, merge_base, branches) -> SimpleMatchingResult:
        super().match_commits(config, output_manager, merge_base, branches)
        """
        This matcher algorithm works in the way described below.
        First, it has some assumptions about the data stored into the BranchData objects.\n

        - 1. The branch objects are set to self.branch_data.

        - 2. Both branches (BranchData objects) are having a property called
        'commits_with_missing_jira_id_filtered'.
        This is a dict of [commit message, CommitData] and this dict should hold all commits after the
        merge-base commit of the compared branches so we don't unnecessarily compare commits below the merge-base.\n

        - 3. Both branches (BranchData objects) have the 'commits_after_merge_base' property
        with commits after the merge-base, similarly to the dict described above,
        but this is a simple list of commits in a particular order which is irrelevant for the algorithm.

        - 4. Both branches (BranchData objects) have the 'jira_id_to_commits' property set and filled
        with commits after the merge-base,
        similarly to the dict described above.
        This property is a dict of Jira IDs (e.g. YARN-1234) mapped to a list of CommitData objects as
        one Jira ID can have multiple associated commits on a branch. \n
        Side note: For the algorithm, it's only important to have this dict filled for the feature branch.

        Note: When we talk about commits, we always mean commits after the merge-base, for simplicity, \n
        the rest of the commits are not relevant for the algorithm at all.

        The algorithm: \n
        1. The main loop iterates over the commits of the master branch.\n
        2. If a particular master commit does not have any Jira ID, the algorithm tries to match the
        commits by message. \n
        It will check if the exact same message is saved to the dict of
        commits_with_missing_jira_id_filtered of the feature branch.
        If yes, the commit is treated as a common commit.
        3. If a particular commit has the Jira ID set, it will be matched against feature branch commits
        with the same Jira ID.
        The actual matching process is the concern of the method of RelatedCommitGroupSimple, called 'process'.
        While executing this loop, all results are saved to self.common_commits, which is a SimpleMatchingResult object.\n
        All matches are stored to self.common_commits.after_merge_base as a Tuple of 2 CommitData objects,
        across the 2 branches.
        For diagnostic and logging purposes, commits matched only by message, only by Jira ID or both are stored to
        self.matched_only_by_message, self.matched_only_by_jira_id and self.matched_both, respectively.

        After the main loop is finished, the common commits are already identified. \n
        We also saved the set of common Jira IDs and a set of common commit messages.\n
        The last remaining step is to iterate over all commits on both branches and
        check against these "common sets".
        If a commit is not in any of the Jira ID-based or commit message-based set,
        it's unique on the particular branch.
        These unique commits will be saved to the 'unique_commits' property of a given branch. As this is a list,
        the algorithm keeps the original ordering of the commits.

        """
        feature_br: BranchData = self.branch_data[BranchType.FEATURE]
        master_br: BranchData = self.branch_data[BranchType.MASTER]

        common_jira_ids: Set[str] = set()
        common_commit_msgs: Set[str] = set()
        master_commits_by_message: Dict[str, List[CommitData]] = master_br.filtered_commits_by_message
        feature_commits_by_message: Dict[str, List[CommitData]] = feature_br.filtered_commits_by_message

        # List of tuples.
        # First item: Master branch CommitData, second item: feature branch CommitData
        for master_commit in master_br.commits_after_merge_base:
            master_jira_id = master_commit.jira_id
            if not master_jira_id:
                # If this commit is without jira id and author was not an item of authors to filter,
                # then try to match commits across branches by commit message.
                self.match_by_commit_message(
                    master_commit, common_commit_msgs, feature_commits_by_message, master_commits_by_message
                )
            elif master_jira_id in feature_br.jira_id_to_commits:
                # Normal path: Try to match commits across branches by Jira ID
                self.match_by_jira_id(common_jira_ids, feature_br, master_commit, master_jira_id)

        for br_data in self.branch_data.values():
            commits_by_msg = (
                master_commits_by_message if br_data.type == BranchType.MASTER else feature_commits_by_message
            )
            self.matching_result.unique_commits[br_data.type] = self._determine_unique_commits(
                br_data.commits_after_merge_base,
                commits_by_msg,
                common_jira_ids,
                common_commit_msgs,
            )
            LOG.info(
                f"Identified {len(self.matching_result.unique_commits[br_data.type])}"
                f" unique commits on branch: {br_data.name}"
            )

        summary_data: SimpleCommitMatcherSummaryData = SimpleCommitMatcherSummaryData(
            config, branches, self.matching_result
        )
        # TODO this is a bug, summary table rendering happens here but new result files will be created afterwards
        self.matching_result.rendered_summary = SimpleRenderedSummary(summary_data, self.matching_result)
        return self.matching_result

    def match_by_commit_message(
        self, master_commit, common_commit_msgs, feature_commits_by_message, master_commits_by_message
    ):
        master_commit_msg = master_commit.message
        if master_commit_msg in master_commits_by_message:
            LOG.debug(
                "Trying to match commit by commit message as Jira ID is missing. \n"
                f"Branch: master branch\n"
                f"Commit: {CommonUtils.convert_commit_to_str(master_commit)}\n"
            )
            # Master commit message found in missing jira id list of the feature branch, record match
            if master_commit_msg in feature_commits_by_message:
                LOG.warning(
                    "Found match by commit message.\n"
                    f"Branch: master branch\n"
                    f"Master branch commit: {CommonUtils.convert_commit_to_str(master_commit)}\n"
                    f"Feature branch commit(s): {CommonUtils.convert_commits_to_oneline_strings(feature_commits_by_message[master_commit_msg])}\n"
                )
                common_commit_msgs.add(master_commit_msg)
                commit_group: RelatedCommitGroupSimple = RelatedCommitGroupSimple(
                    [master_commit], feature_commits_by_message[master_commit_msg]
                )
                # ATM, these are groups that contain 1 master / 1 feature commit
                self.matching_result.after_merge_base.extend(commit_group.get_matched_by_msg)
                self.matching_result.matched_only_by_message.extend(commit_group.get_matched_by_msg)

    def match_by_jira_id(self, common_jira_ids, feature_br, master_commit, master_jira_id):
        feature_commits: List[CommitData] = feature_br.jira_id_to_commits[master_jira_id]
        LOG.debug(
            "Found matching commits by Jira ID. Details: \n"
            f"Master branch commit: {master_commit.as_oneline_string()}\n"
            f"Feature branch commits: {[fc.as_oneline_string() for fc in feature_commits]}"
        )
        commit_group = RelatedCommitGroupSimple([master_commit], feature_commits)
        self.matching_result.matched_both.extend(commit_group.get_matched_by_id_and_msg)
        self.matching_result.matched_only_by_jira_id.extend(commit_group.get_matched_by_id)
        # Either if commit message matched or not, count this as a common commit as Jira ID matched
        self.matching_result.after_merge_base.extend(commit_group.get_matched_by_id)
        common_jira_ids.add(master_jira_id)

    @staticmethod
    def _determine_unique_commits(
        commits: List[CommitData],
        commits_by_message: Dict[str, List[CommitData]],
        common_jira_ids: Set[str],
        common_commit_msgs: Set[str],
    ) -> List[CommitData]:
        result: List[CommitData] = []
        # 1. Values of commit list can contain commits without Jira ID
        # and we don't want to count them as unique commits unless the commit is a
        # special authored commit and it's not a common commit by its message
        # 2. If Jira ID is in common_jira_ids, it's not a unique commit, either.
        for commit in commits:
            special_unique_commit: bool = (
                not commit.jira_id and commit.message in commits_by_message and commit.message not in common_commit_msgs
            )
            normal_unique_commit: bool = commit.jira_id is not None and commit.jira_id not in common_jira_ids
            if special_unique_commit or normal_unique_commit:
                result.append(commit)
        return result


class RelatedCommitGroupSimple:
    def __init__(self, master_commits: List[CommitData], feature_commits: List[CommitData]):
        self.master_commits = master_commits
        self.feature_commits = feature_commits
        self.match_data: Dict[CommitMatchType, List[Tuple[CommitData, CommitData]]] = self.process()

    @property
    def get_matched_by_id_and_msg(self) -> List[Tuple[CommitData, CommitData]]:
        return self.match_data[CommitMatchType.MATCHED_BY_BOTH]

    @property
    def get_matched_by_id(self) -> List[Tuple[CommitData, CommitData]]:
        return self.match_data[CommitMatchType.MATCHED_BY_ID]

    @property
    def get_matched_by_msg(self) -> List[Tuple[CommitData, CommitData]]:
        return self.match_data[CommitMatchType.MATCHED_BY_MESSAGE]

    def process(self):
        result_dict = {cmt: [] for cmt in CommitMatchType}
        # We can assume one master commit for this implementation
        mc = self.master_commits[0]
        result: List[CommitData]
        for fc in self.feature_commits:
            match_by_id = mc.jira_id == fc.jira_id
            match_by_msg = mc.message == fc.message
            if match_by_id and match_by_msg:
                result_dict[CommitMatchType.MATCHED_BY_BOTH].append((mc, fc))
            elif match_by_id:
                result_dict[CommitMatchType.MATCHED_BY_ID].append((mc, fc))
            elif match_by_msg:
                LOG.warning(
                    "Jira ID is the same for commits, but commit message differs: \n"
                    f"Master branch commit: {mc.as_oneline_string()}\n"
                    f"Feature branch commit: {fc.as_oneline_string()}"
                )
                result_dict[CommitMatchType.MATCHED_BY_MESSAGE].append((mc, fc))
        return result_dict


class SimpleOutputManager(OutputManagerAbs):
    def write_commit_list_to_file_or_console(
        self,
        output_type: str,
        commit_groups: List[Tuple[CommitData, CommitData]],
        add_sep_to_end=True,
        add_line_break_between_groups=False,
    ):
        if not add_line_break_between_groups:
            commits = [CommonUtils.convert_commit_to_str(commit) for tup in commit_groups for commit in tup]
            contents = StringUtils.list_to_multiline_string(commits)
        else:
            contents = ""
            for tup in commit_groups:
                commit_strs = [CommonUtils.convert_commit_to_str(commit) for commit in tup]
                contents += StringUtils.list_to_multiline_string(commit_strs)
                contents += "\n\n"

        self._write_to_file_or_console(contents, output_type, add_sep_to_end=add_sep_to_end)

    def write_commit_match_result_files(
        self, branch_data: Dict[BranchType, BranchData], matching_result: MatchingResultBase
    ):
        matching_result = typing.cast(SimpleMatchingResult, matching_result)
        self.write_commit_list_to_file_or_console(
            "commit message differs",
            matching_result.matched_only_by_jira_id,
            add_sep_to_end=False,
            add_line_break_between_groups=True,
        )

        self.write_commit_list_to_file_or_console(
            "commits matched by message",
            matching_result.matched_only_by_message,
            add_sep_to_end=False,
            add_line_break_between_groups=True,
        )
        for br_data in branch_data.values():
            self.write_to_file_or_console("unique commits", br_data, matching_result.unique_commits[br_data.type])


class SimpleRenderedSummary(RenderedSummaryAbs):
    def __init__(self, summary_data, matching_result):
        # TODO list of RenderedTableType: Error-prone as if any of it is missing, rendering will be wrong
        super().__init__(
            summary_data,
            matching_result,
            [
                RenderedTableType.RESULT_FILES,
                RenderedTableType.UNIQUE_ON_BRANCH,
                RenderedTableType.COMMON_COMMITS_SINCE_DIVERGENCE,
                RenderedTableType.ALL_COMMITS_MERGED,
            ],
        )

        self.add_result_files_table()
        self.add_unique_commit_tables(matching_result)
        self.add_matched_commits_table()
        self.add_all_commits_tables()
        self.printable_summary_str, self.writable_summary_str, self.html_summary = self.generate_summary_msgs()

    def add_matched_commits_table(self):
        table_type = RenderedTableType.COMMON_COMMITS_SINCE_DIVERGENCE
        header = [HEADER_ROW, HEADER_JIRA_ID, HEADER_COMMIT_MSG, HEADER_COMMIT_DATE, HEADER_COMMITTER]
        source_data = self.summary_data.common_commits_after_merge_base()

        render_conf = TableRenderingConfig(
            row_callback=lambda commit: (commit.jira_id, commit.message, commit.date, commit.committer),
            print_result=False,
            max_width=80,
            max_width_separator=" ",
            add_row_numbers=False,
            tabulate_formats=DEFAULT_TABLE_FORMATS,
        )
        gen_tables = ResultPrinter.print_tables(source_data, header=header, render_conf=render_conf)
        for table_fmt, table in gen_tables.items():
            self.add_table(
                table_type,
                TableWithHeader(table_type.header, header, source_data, table, table_fmt=table_fmt, colorized=False),
            )

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

        render_conf = TableRenderingConfig(
            row_callback=lambda row: row,
            print_result=False,
            max_width=100,
            max_width_separator=" ",
            add_row_numbers=False,
            tabulate_formats=DEFAULT_TABLE_FORMATS,
            bool_conversion_config=BoolConversionConfig(),
            colorize_config=colorize_conf,
        )
        gen_tables = ResultPrinter.print_tables(all_commits, header=header, render_conf=render_conf)
        for table_fmt, table in gen_tables.items():
            self.add_table(
                table_type,
                TableWithHeader(table_type.header, header, all_commits, table, table_fmt=table_fmt, colorized=colorize),
            )
