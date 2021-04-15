import logging
from typing import Set, Dict, List, Tuple

from yarndevtools.commands.branchcomparator.common import BranchData, BranchType, CommonCommitsBase, RelatedCommitGroup
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


class SimpleCommitMatcherSummaryData(SummaryDataAbs):
    def common_commits_after_merge_base(self):
        return [c[0] for c in self._common_commits.after_merge_base]

    def add_stats_common_commit_details(self, res):
        res += "\n\n=====Stats: COMMON COMMITS ACROSS BRANCHES=====\n"
        res += (
            f"Number of common commits with missing Jira ID, matched by commit message: "
            f"{len(self._common_commits.matched_only_by_message)}\n"
        )
        res += (
            f"Number of common commits with matching Jira ID but different commit message: "
            f"{len(self._common_commits.matched_only_by_jira_id)}\n"
        )
        res += (
            f"Number of common commits with matching Jira ID and commit message: "
            f"{len(self._common_commits.matched_both)}\n"
        )
        return res

    def add_stats_common_commits_on_branches(self, res):
        res += "\n\n=====Stats: COMMON=====\n"
        res += f"Merge-base commit: {self.merge_base.hash} {self.merge_base.message} {self.merge_base.date}\n"
        res += f"Number of common commits before merge-base: {len(self._common_commits.before_merge_base)}\n"
        res += f"Number of common commits after merge-base: {len(self._common_commits.after_merge_base)}\n"
        return res


class SimpleCommitMatcher:
    def __init__(self, branch_data: Dict[BranchType, BranchData]):
        self.branch_data = branch_data
        self.common_commits: CommonCommits or None = None

    def create_common_commits_obj(self) -> CommonCommits:
        self.common_commits = CommonCommits()
        return self.common_commits

    def create_summary_data(self, config, branches) -> SummaryDataAbs:
        return SimpleCommitMatcherSummaryData(config, branches)

    def match_commits(self) -> CommonCommits:
        feature_br: BranchData = self.branch_data[BranchType.FEATURE]
        master_br: BranchData = self.branch_data[BranchType.MASTER]

        common_jira_ids: Set[str] = set()
        common_commit_msgs: Set[str] = set()
        master_commits_by_message: Dict[str, CommitData] = master_br.commits_with_missing_jira_id_filtered
        feature_commits_by_message: Dict[str, CommitData] = feature_br.commits_with_missing_jira_id_filtered

        # List of tuples.
        # First item: Master branch CommitData, second item: feature branch CommitData
        for master_commit in master_br.commits_after_merge_base:
            master_commit_msg = master_commit.message
            master_jira_id = master_commit.jira_id
            if not master_jira_id:
                # If this commit is without jira id and author was not an item of authors to filter,
                # then try to match commits across branches by commit message.
                if master_commit_msg in master_commits_by_message:
                    LOG.debug(
                        "Trying to match commit by commit message as Jira ID is missing. Details: \n"
                        f"Branch: master branch\n"
                        f"Commit message: ${master_commit_msg}\n"
                    )
                    # Master commit message found in missing jira id list of the feature branch, record match
                    if master_commit_msg in feature_commits_by_message:
                        LOG.warning(
                            "Found matching commit by commit message. Details: \n"
                            f"Branch: master branch\n"
                            f"Commit message: ${master_commit_msg}\n"
                        )
                        common_commit_msgs.add(master_commit_msg)
                        commit_group: RelatedCommitGroup = RelatedCommitGroup(
                            [master_commit], [feature_commits_by_message[master_commit_msg]]
                        )
                        # ATM, these are groups that contain 1 master / 1 feature commit
                        self.common_commits.after_merge_base.extend(commit_group.get_matched_by_msg)
                        self.common_commits.matched_only_by_message.extend(commit_group.get_matched_by_msg)

            elif master_jira_id in feature_br.jira_id_to_commits:
                # Normal path: Try to match commits across branches by Jira ID
                feature_commits: List[CommitData] = feature_br.jira_id_to_commits[master_jira_id]
                LOG.debug(
                    "Found matching commits by jira id. Details: \n"
                    f"Master branch commit: {master_commit.as_oneline_string()}\n"
                    f"Feature branch commits: {[fc.as_oneline_string() for fc in feature_commits]}"
                )

                commit_group = RelatedCommitGroup([master_commit], feature_commits)
                self.common_commits.matched_both.extend(commit_group.get_matched_by_id_and_msg)
                self.common_commits.matched_only_by_jira_id.extend(commit_group.get_matched_by_id)

                # Either if commit message matched or not, count this as a common commit as Jira ID matched
                self.common_commits.after_merge_base.extend(commit_group.get_matched_by_id)
                common_jira_ids.add(master_jira_id)

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

    @staticmethod
    def _determine_unique_commits(
        commits: List[CommitData], commits_by_message: Dict[str, CommitData], common_jira_ids, common_commit_msgs
    ) -> List[CommitData]:
        result = []
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
