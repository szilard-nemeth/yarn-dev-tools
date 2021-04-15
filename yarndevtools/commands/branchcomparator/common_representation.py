from abc import ABC
from typing import List, Dict, Any, Tuple

from yarndevtools.commands.branchcomparator.common import BranchType, BranchData
from yarndevtools.commands_common import CommitData


# TODO Make a subclass for SimpleCommitMatcher and other implementations
class SummaryDataAbs(ABC):
    def __init__(self, conf, branches: Any):
        self.output_dir: str = conf.output_dir
        self.run_legacy_script: bool = conf.run_legacy_script
        self.merge_base: CommitData or None = None

        # Dict-based data structure, key: BranchType
        # These are set before comparing the branches
        self.branch_data: Dict[BranchType, BranchData] = branches.branch_data

        # TODO remove this when possible
        self._common_commits = None

    @property
    def common_commits(self):
        return [c[0] for c in self._common_commits.after_merge_base]

    @property
    def all_commits(self):
        all_commits: List[CommitData] = (
            []
            + self.branch_data[BranchType.MASTER].unique_commits
            + self.branch_data[BranchType.FEATURE].unique_commits
            + self.common_commits
        )
        all_commits.sort(key=lambda c: c.date, reverse=True)
        return all_commits

    @property
    def all_commits_with_missing_jira_id(self) -> Dict[BranchType, List[CommitData]]:
        result = {}
        for br_type, br_data in self.branch_data.items():
            result[br_type] = br_data.all_commits_with_missing_jira_id
        return result

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

    def add_stats_commits_with_missing_jira_id(self, res):
        for br_type, br_data in self.branch_data.items():
            res += f"\n\n=====Stats: COMMITS WITH MISSING JIRA ID ON BRANCH: {br_data.name}=====\n"
            res += (
                f"Number of all commits with missing Jira ID: {len(self.all_commits_with_missing_jira_id[br_type])}\n"
            )
            res += (
                f"Number of commits with missing Jira ID after merge-base: "
                f"{len(br_data.commits_with_missing_jira_id)}\n"
            )
            res += (
                f"Number of commits with missing Jira ID after merge-base, filtered by author exceptions: "
                f"{len(br_data.commits_with_missing_jira_id_filtered)}\n"
            )
        return res

    def add_stats_common_commits_on_branches(self, res):
        res += "\n\n=====Stats: COMMON=====\n"
        res += f"Merge-base commit: {self.merge_base.hash} {self.merge_base.message} {self.merge_base.date}\n"
        res += f"Number of common commits before merge-base: {len(self._common_commits.before_merge_base)}\n"
        res += f"Number of common commits after merge-base: {len(self._common_commits.after_merge_base)}\n"
        return res

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
