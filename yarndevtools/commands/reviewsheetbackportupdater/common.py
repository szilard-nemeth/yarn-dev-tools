from typing import List, Dict, Set

from pythoncommons.string_utils import auto_str

from yarndevtools.commands_common import BackportedJira


@auto_str
class ReviewSheetBackportUpdaterData:
    def __init__(self):
        self.jira_ids: List[str] = []
        self.backported_jiras: Dict[str, BackportedJira] = {}
        self.backported_to_branches: Dict[str, Set[str]] = {}
        self.commits_of_jira: Dict[str, Set[str]] = {}
