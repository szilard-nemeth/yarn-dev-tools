import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Dict, Set
from yarndevtools.commands_common import CommitData

LOG = logging.getLogger(__name__)


class BranchType(Enum):
    FEATURE = "feature branch"
    MASTER = "master branch"


class CommitMatchType(Enum):
    MATCHED_BY_MESSAGE = "matched_by_msg"
    MATCHED_BY_ID = "matched_by_id"
    MATCHED_BY_BOTH = "matched_by_both"


class MatchingResultBase(ABC):
    def __init__(self):
        self.before_merge_base: List[CommitData] = []


class CommitMatcherBase(ABC):
    @abstractmethod
    def create_matching_result(self) -> MatchingResultBase:
        pass


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

        # Dict key: commit hash, value: CommitData obj
        self.commits_with_missing_jira_id_filtered: Dict[str, CommitData] = {}

        self.commits_before_merge_base: List[CommitData] = []
        self.commits_after_merge_base: List[CommitData] = []

        # Commits filtered by author exceptions (may contain commits with missing Jira ID)
        self.commits_after_merge_base_filtered: List[CommitData] = []

        # Dict: commit hash to commit index
        self.hash_to_index: Dict[str, int] = {}

        # Dict: Jira ID (e.g. YARN-1234) to List of CommitData objects
        self.jira_id_to_commits: Dict[str, List[CommitData]] = {}
        self.unique_commits: List[CommitData] = []
        self.merge_base_idx: int = -1
        self.unique_jira_ids_legacy_script: List[str] = []

    def __str__(self):
        return f"Branch type: {self.type}"

    def __repr__(self):
        return f"Branch type: {self.type}"

    def get_commit_hashes(self) -> Set[str]:
        # Searching through the commits after merge base, filtered (removed commits with "to filter" authors)
        return set([commit.hash for commit in self.commits_after_merge_base_filtered])

    def get_commits_by_hashes(self, c_hashes: Set[str]) -> Dict[str, CommitData]:
        # Searching through the commits after merge base, filtered (removed commits with "to filter" authors)
        hash_to_commit_dict = dict([(c.hash, c) for c in self.commits_after_merge_base_filtered])
        return {hash: commit for hash, commit in hash_to_commit_dict.items() if hash in c_hashes}

    @property
    def filtered_commit_list(self) -> List[CommitData]:
        return [c for c in self.commits_with_missing_jira_id_filtered.values()]

    @property
    def filtered_commits_by_message(self) -> Dict[str, List[CommitData]]:
        # We may have more commits for a commit message
        result_dict: Dict[str, List[CommitData]] = {}
        for commit in self.commits_with_missing_jira_id_filtered.values():
            if commit.message not in result_dict:
                result_dict[commit.message] = []
            result_dict[commit.message].append(commit)
        return result_dict

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
