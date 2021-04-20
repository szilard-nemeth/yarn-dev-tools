import logging
from typing import Set, Dict, List, Tuple

from yarndevtools.commands.branchcomparator.common import (
    BranchData,
    BranchType,
    CommonCommitsBase,
    convert_commit_to_str,
    convert_commits_to_oneline_strings,
)
from yarndevtools.commands.branchcomparator.common_representation import SummaryDataAbs
from yarndevtools.commands_common import CommitData

LOG = logging.getLogger(__name__)


class CommonCommits(CommonCommitsBase):
    def __init__(self):
        super().__init__()
        self.after_merge_base: List[Tuple[CommitData, CommitData]] = []

        # Commits matched by message with missing Jira ID
        self.matched_only_by_message: List[Tuple[CommitData, CommitData]] = []

        # Commits matched by Jira ID but not by message
        self.matched_only_by_jira_id: List[Tuple[CommitData, CommitData]] = []

        # Commits matched by Jira ID and by message as well
        self.matched_both: List[Tuple[CommitData, CommitData]] = []

    @property
    def commits_after_merge_base(self):
        return [c[0] for c in self.after_merge_base]


class SimpleCommitMatcherSummaryData(SummaryDataAbs):
    def __init__(self, config, branches, common_commits):
        super().__init__(config, branches)
        self.common_commits = common_commits

    def common_commits_after_merge_base(self):
        return self.common_commits.commits_after_merge_base

    def add_stats_common_commit_details(self, res):
        res += "\n\n=====Stats: COMMON COMMITS ACROSS BRANCHES=====\n"
        res += (
            f"Number of common commits with missing Jira ID, matched by commit message: "
            f"{len(self.common_commits.matched_only_by_message)}\n"
        )
        res += (
            f"Number of common commits with matching Jira ID but different commit message: "
            f"{len(self.common_commits.matched_only_by_jira_id)}\n"
        )
        res += (
            f"Number of common commits with matching Jira ID and commit message: "
            f"{len(self.common_commits.matched_both)}\n"
        )
        return res

    def add_stats_common_commits_on_branches(self, res):
        res += "\n\n=====Stats: COMMON=====\n"
        res += f"Merge-base commit: {self.branches.merge_base.as_oneline_string(incl_date=True)}\n"
        res += f"Number of common commits before merge-base: {len(self.common_commits.before_merge_base)}\n"
        res += f"Number of common commits after merge-base: {len(self.common_commits.after_merge_base)}\n"
        return res


class SimpleCommitMatcher:
    def __init__(self, branch_data: Dict[BranchType, BranchData]):
        self.branch_data = branch_data
        self.common_commits: CommonCommits or None = None

    def create_common_commits_obj(self) -> CommonCommits:
        self.common_commits = CommonCommits()
        return self.common_commits

    def create_summary_data(self, config, branches, common_commits) -> SummaryDataAbs:
        return SimpleCommitMatcherSummaryData(config, branches, common_commits)

    def match_commits(self) -> CommonCommits:
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
        While executing this loop, all results are saved to self.common_commits, which is a CommonCommits object.\n
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
            br_data.unique_commits = self._determine_unique_commits(
                br_data.commits_after_merge_base,
                commits_by_msg,
                common_jira_ids,
                common_commit_msgs,
            )
            LOG.info(f"Identified {len(br_data.unique_commits)} unique commits on branch: {br_data.name}")

        return self.common_commits

    def match_by_commit_message(
        self, master_commit, common_commit_msgs, feature_commits_by_message, master_commits_by_message
    ):
        master_commit_msg = master_commit.message
        if master_commit_msg in master_commits_by_message:
            LOG.debug(
                "Trying to match commit by commit message as Jira ID is missing. \n"
                f"Branch: master branch\n"
                f"Commit: {convert_commit_to_str(master_commit)}\n"
            )
            # Master commit message found in missing jira id list of the feature branch, record match
            if master_commit_msg in feature_commits_by_message:
                LOG.warning(
                    "Found match by commit message.\n"
                    f"Branch: master branch\n"
                    f"Master branch commit: {convert_commit_to_str(master_commit)}\n"
                    f"Feature branch commit(s): {convert_commits_to_oneline_strings(feature_commits_by_message[master_commit_msg])}\n"
                )
                common_commit_msgs.add(master_commit_msg)
                commit_group: RelatedCommitGroupSimple = RelatedCommitGroupSimple(
                    [master_commit], feature_commits_by_message[master_commit_msg]
                )
                # ATM, these are groups that contain 1 master / 1 feature commit
                self.common_commits.after_merge_base.extend(commit_group.get_matched_by_msg)
                self.common_commits.matched_only_by_message.extend(commit_group.get_matched_by_msg)

    def match_by_jira_id(self, common_jira_ids, feature_br, master_commit, master_jira_id):
        feature_commits: List[CommitData] = feature_br.jira_id_to_commits[master_jira_id]
        LOG.debug(
            "Found matching commits by Jira ID. Details: \n"
            f"Master branch commit: {master_commit.as_oneline_string()}\n"
            f"Feature branch commits: {[fc.as_oneline_string() for fc in feature_commits]}"
        )
        commit_group = RelatedCommitGroupSimple([master_commit], feature_commits)
        self.common_commits.matched_both.extend(commit_group.get_matched_by_id_and_msg)
        self.common_commits.matched_only_by_jira_id.extend(commit_group.get_matched_by_id)
        # Either if commit message matched or not, count this as a common commit as Jira ID matched
        self.common_commits.after_merge_base.extend(commit_group.get_matched_by_id)
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
    MATCHED_BY_MSG = "matched_by_msg"
    MATCHED_BY_ID = "matched_by_id"
    MATCHED_BY_BOTH = "matched_by_both"

    def __init__(self, master_commits: List[CommitData], feature_commits: List[CommitData]):
        self.master_commits = master_commits
        self.feature_commits = feature_commits
        self.match_data: Dict[str, List[Tuple[CommitData, CommitData]]] = self.process()

    @property
    def get_matched_by_id_and_msg(self) -> List[Tuple[CommitData, CommitData]]:
        return self.match_data[self.MATCHED_BY_BOTH]

    @property
    def get_matched_by_id(self) -> List[Tuple[CommitData, CommitData]]:
        return self.match_data[self.MATCHED_BY_ID]

    @property
    def get_matched_by_msg(self) -> List[Tuple[CommitData, CommitData]]:
        return self.match_data[self.MATCHED_BY_MSG]

    def process(self):
        result_dict = {self.MATCHED_BY_ID: [], self.MATCHED_BY_MSG: [], self.MATCHED_BY_BOTH: []}
        # We can assume one master commit for this implementation
        mc = self.master_commits[0]
        result: List[CommitData]
        for fc in self.feature_commits:
            match_by_id = mc.jira_id == fc.jira_id
            match_by_msg = mc.message == fc.message
            if match_by_id and match_by_msg:
                result_dict[self.MATCHED_BY_BOTH].append((mc, fc))
            elif match_by_id:
                result_dict[self.MATCHED_BY_ID].append((mc, fc))
            elif match_by_msg:
                LOG.warning(
                    "Jira ID is the same for commits, but commit message differs: \n"
                    f"Master branch commit: {mc.as_oneline_string()}\n"
                    f"Feature branch commit: {fc.as_oneline_string()}"
                )
                result_dict[self.MATCHED_BY_MSG].append((mc, fc))
        return result_dict
