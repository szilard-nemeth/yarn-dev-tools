from enum import Enum
from typing import List, Dict

from yarndevtools.commands_common import CommitData


class BranchType(Enum):
    FEATURE = "feature branch"
    MASTER = "master branch"


class BranchData:
    def __init__(self, type: BranchType, branch_name: str):
        self.type: BranchType = type
        self.name: str = branch_name
        self.shortname = branch_name.split("/")[1] if "/" in branch_name else branch_name

        # Set later
        self.gitlog_results: List[str] = []
        # CommitData objects stored in a list, ordered from last to first commit (descending, from oldest to newest)
        self.commit_objs: List[CommitData] = []

        self.all_commits_with_missing_jira_id: List[CommitData] = []
        self.commits_with_missing_jira_id: List[CommitData] = []

        # Dict key: commit message, value: CommitData obj
        self.commits_with_missing_jira_id_filtered: Dict[str, CommitData] = {}

        self.commits_before_merge_base: List[CommitData] = []
        self.commits_after_merge_base: List[CommitData] = []

        # Dict: commit hash to commit index
        self.hash_to_index: Dict[str, int] = {}

        # Dict: Jira ID (e.g. YARN-1234) to List of CommitData objects
        self.jira_id_to_commits: Dict[str, List[CommitData]] = {}
        self.unique_commits: List[CommitData] = []
        self.merge_base_idx: int = -1
        self.unique_jira_ids_legacy_script: List[str] = []

    @property
    def number_of_commits(self):
        if not self.gitlog_results:
            raise ValueError("Git log is not yet queried so number of commits is not yet stored.")
        return len(self.gitlog_results)

    def set_merge_base(self, merge_base: CommitData):
        merge_base_hash = merge_base.hash
        if merge_base_hash not in self.hash_to_index:
            raise ValueError("Merge base cannot be found among commits. Merge base hash: " + merge_base_hash)
        self.merge_base_idx = self.hash_to_index[merge_base_hash]

        if len(self.commit_objs) == 0:
            raise ValueError("set_merge_base is invoked while commit list was empty!")
        self.commits_before_merge_base = self.commit_objs[: self.merge_base_idx]
        self.commits_after_merge_base = self.commit_objs[self.merge_base_idx :]

    def set_commit_objs(self, commits):
        self.commit_objs = commits
        self.all_commits_with_missing_jira_id = list(filter(lambda c: not c.jira_id, self.commit_objs))
