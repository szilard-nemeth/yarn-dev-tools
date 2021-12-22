from enum import Enum

from pythoncommons.string_utils import auto_str


@auto_str
class JiraUmbrellaData:
    # @auto_str(exclude_props=["jira_html"]) #TODO make this work
    def __init__(self):
        self.subjira_ids = []
        self.jira_ids_and_titles = {}
        self.jira_html = None
        self.piped_jira_ids = None
        self.matched_upstream_commit_list = None
        self.matched_upstream_commit_hashes = None
        self.list_of_changed_files = None
        self.upstream_commit_data_list = None
        self.execution_mode = None
        self.backported_jiras = dict()

    @property
    def no_of_matched_commits(self):
        return len(self.matched_upstream_commit_list)

    @property
    def no_of_jiras(self):
        return len(self.subjira_ids)

    @property
    def no_of_commits(self):
        return len(self.matched_upstream_commit_hashes)

    @property
    def no_of_files(self):
        return len(self.list_of_changed_files)


class ExecutionMode(Enum):
    AUTO_BRANCH_MODE = "auto_branch_mode"
    MANUAL_BRANCH_MODE = "manual_branch_mode"
