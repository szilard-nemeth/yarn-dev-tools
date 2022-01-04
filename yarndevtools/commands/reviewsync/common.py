from typing import List, Dict, Set, OrderedDict

from pythoncommons.jira_wrapper import AdvancedJiraPatch, PatchApply
from pythoncommons.string_utils import auto_str


@auto_str
class ReviewsyncData:
    def __init__(self):
        self.issues: List[str] = []
        self.commit_branches_for_issues: Dict[str, Set[str]] = {}
        self.patches_for_issues: Dict[str, List[AdvancedJiraPatch]] = {}
        self.patch_applies_for_issues: Dict[str, List[PatchApply]] = OrderedDict()
