from dataclasses import dataclass
from enum import Enum
from typing import List, Dict

from pythoncommons.string_utils import auto_str

from yarndevtools.commands_common import CommitData, GitLogLineFormat


@dataclass
class UpstreamCommitsPerBranch:
    branch: str
    matched_upstream_commit_list: List[str] or None = None
    matched_upstream_commit_hashes: List[str] or None = None
    matched_upstream_commitdata_list: List[CommitData] or None = None

    def __post_init__(self):
        self.convert_to_commit_data_objects_upstream()

    def convert_to_commit_data_objects_upstream(self):
        """
        Iterate over commit hashes, print the following to summary_file for each commit hash:
        <hash> <YARN-id> <commit date>
        :return:
        """
        self.matched_upstream_commitdata_list = [
            CommitData.from_git_log_str(commit_str, format=GitLogLineFormat.ONELINE_WITH_DATE)
            for commit_str in self.matched_upstream_commit_list
        ]
        self.matched_upstream_commit_hashes = [commit_obj.hash for commit_obj in self.matched_upstream_commitdata_list]

    @property
    def no_of_matched_commits(self):
        return len(self.matched_upstream_commit_list)

    @property
    def no_of_commits(self):
        return len(self.matched_upstream_commit_hashes)


@auto_str
class JiraUmbrellaData:
    # @auto_str(exclude_props=["jira_html"]) #TODO make this work
    def __init__(self):
        self.subjira_ids: List[str] = []
        self.jira_ids_and_titles: Dict[str, str] = {}
        self.jira_html: str or None = None
        self.piped_jira_ids: str or None = None
        self.list_of_changed_files: List[str] or None = None
        self.execution_mode: ExecutionMode or None = None
        self.backported_jiras: Dict[str, BackportedJira] = {}  # Key: Jira ID
        self.upstream_commits_by_branch: Dict[str, UpstreamCommitsPerBranch] = {}  # Key: branch name

    @property
    def no_of_jiras(self):
        return len(self.subjira_ids)

    @property
    def no_of_files(self):
        return len(self.list_of_changed_files)


class ExecutionMode(Enum):
    AUTO_BRANCH_MODE = "auto_branch_mode"
    MANUAL_BRANCH_MODE = "manual_branch_mode"


@auto_str
class BackportedJira:
    def __init__(self, jira_id, commits):
        self.jira_id = jira_id
        self.commits = commits
