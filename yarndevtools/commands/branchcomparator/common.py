import logging
from abc import ABC
from enum import Enum
from typing import List, Dict, Tuple

from pythoncommons.string_utils import auto_str

from yarndevtools.commands_common import CommitData

LOG = logging.getLogger(__name__)


class BranchType(Enum):
    FEATURE = "feature branch"
    MASTER = "master branch"


class CommonCommitsBase(ABC):
    def __init__(self):
        self.before_merge_base: List[CommitData] = []


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

    def __str__(self):
        return f"Branch type: {self.type}"

    def __repr__(self):
        return f"Branch type: {self.type}"

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


class RelatedCommitGroup:
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
        # TODO we can assume one master commit for now
        mc = self.master_commits[0]
        result: List[CommitData]
        for fc in self.feature_commits:
            match_by_id = mc.jira_id == fc.jira_id
            match_by_msg = mc.message == fc.message
            if match_by_id and match_by_msg:
                result_dict[self.MATCHED_BY_BOTH].append((mc, fc))
            elif match_by_id:
                result_dict[self.MATCHED_BY_ID].append((mc, fc))
            elif match_by_id:
                LOG.warning(
                    "Jira ID is the same for commits, but commit message differs: \n"
                    f"Master branch commit: {mc.as_oneline_string()}\n"
                    f"Feature branch commit: {fc.as_oneline_string()}"
                )
                result_dict[self.MATCHED_BY_MSG].append((mc, fc))
        return result_dict


class RelatedCommits:
    def __init__(self):
        self.lst: List[RelatedCommitGroup] = []

    def add(self, cg: RelatedCommitGroup):
        self.lst.append(cg)
