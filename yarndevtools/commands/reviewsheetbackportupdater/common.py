from typing import List, Dict

from pythoncommons.string_utils import auto_str

from yarndevtools.commands_common import BackportedJira


@auto_str
class ReviewSheetBackportUpdaterData:
    def __init__(self):
        self.jira_ids: List[str] = []
        self.backported_jiras: Dict[str, BackportedJira] = {}
