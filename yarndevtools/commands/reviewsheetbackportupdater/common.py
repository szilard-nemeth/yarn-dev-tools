from typing import List, Dict, Set

from pythoncommons.string_utils import auto_str

from yarndevtools.commands_common import BackportedJira, CommitData


@auto_str
class ReviewSheetBackportUpdaterData:
    def __init__(self):
        self.jira_ids: List[str] = []
        self.backported_jiras: Dict[str, BackportedJira] = {}  # Key: Jira ID, Value: BackportedJira
        self.backported_to_branches: Dict[str, Set[str]] = {}  # Key: Jira ID, Value: Set of branch names
        self.commit_hashes_by_branch: Dict[str, Set[str]] = {}  # Key: branch, Value: Set of commit hashes
        self.commits_of_jira: Dict[str, Set[CommitData]] = {}  # Key: Jira ID, Value: Set of CommitData

    def add_commit(self, commit, branch):
        if commit.hash not in self.commit_hashes_by_branch:
            self.commit_hashes_by_branch[commit.hash] = set()
        self.commit_hashes_by_branch[commit.hash].add(branch)

    def is_single_commit(self, issue_id):
        return len(self.commits_of_jira[issue_id]) == 1

    def get_single_commit(self, issue_id):
        commit_data_objs = self.commits_of_jira[issue_id]
        if len(commit_data_objs) > 1:
            raise ValueError(
                "This method should be only invoked in conjunction with 'is_single_commit', and only if it returned True!"
            )
        return list(commit_data_objs)[0]
