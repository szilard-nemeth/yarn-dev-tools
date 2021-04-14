import logging
import os
from enum import Enum
from typing import Dict, List, Tuple, Set, Any

from bs4 import BeautifulSoup
from git import Commit
from pythoncommons.date_utils import DateUtils
from pythoncommons.file_utils import FileUtils
from pythoncommons.process import CommandRunner
from pythoncommons.result_printer import (
    ResultPrinter,
    ColorizeConfig,
    ColorDescriptor,
    Color,
    MatchType,
    EvaluationMethod,
    BoolConversionConfig,
    TabulateTableFormat,
    DEFAULT_TABLE_FORMATS,
)
from pythoncommons.string_utils import StringUtils
from yarndevtools.commands_common import (
    CommitData,
    GitLogLineFormat,
    GitLogParseConfig,
    MatchAllJiraIdStrategy,
    JiraIdTypePreference,
    JiraIdChoosePreference,
)
from yarndevtools.constants import ANY_JIRA_ID_PATTERN, REPO_ROOT_DIRNAME, SUMMARY_FILE_TXT, SUMMARY_FILE_HTML
from pythoncommons.git_wrapper import GitWrapper

HEADER_COMMIT_DATE = "Commit date"
HEADER_COMMIT_MSG = "Commit message"
HEADER_JIRA_ID = "Jira ID"
HEADER_ROW = "Row"
HEADER_FILE = "File"
HEADER_NO_OF_LINES = "# of lines"
HEADER_COMMITTER = "Committer"

LOG = logging.getLogger(__name__)


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
        # Commit objects in reverse order (from oldest to newest)
        # Commits stored in a list, in order from last to first commit (descending)
        self.commit_objs: List[CommitData] = []
        self.commits_before_merge_base: List[CommitData] = []
        self.commits_after_merge_base: List[CommitData] = []
        self.hash_to_index: Dict[str, int] = {}  # Dict: commit hash to commit index
        self.jira_id_to_commits: Dict[
            str, List[CommitData]
        ] = {}  # Dict: Jira ID (e.g. YARN-1234) to List of CommitData objects
        self.unique_commits: List[CommitData] = []
        self.merge_base_idx: int = -1

    @property
    def number_of_commits(self):
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


class RelatedCommitGroup:
    def __init__(self, master_commits: List[CommitData], feature_commits: List[CommitData]):
        self.master_commits = master_commits
        self.feature_commits = feature_commits
        self.match_data: Dict[str, List[Tuple[CommitData, CommitData]]] = self.process()

    @property
    def get_matched_by_id_and_msg(self) -> List[Tuple[CommitData, CommitData]]:
        return self.match_data["matched_by_both"]

    @property
    def get_matched_by_id(self) -> List[Tuple[CommitData, CommitData]]:
        return self.match_data["matched_by_id"]

    @property
    def get_matched_by_msg(self) -> List[Tuple[CommitData, CommitData]]:
        return self.match_data["matched_by_msg"]

    def process(self):
        result_dict = {"matched_by_id": [], "matched_by_msg": [], "matched_by_both": []}
        # TODO we can assume one master commit for now
        mc = self.master_commits[0]
        result: List[CommitData]
        for fc in self.feature_commits:
            match_by_id = mc.jira_id == fc.jira_id
            match_by_msg = mc.message == fc.message
            if match_by_id and match_by_msg:
                result_dict["matched_by_both"].append((mc, fc))
            elif match_by_id:
                result_dict["matched_by_id"].append((mc, fc))
            elif match_by_id:
                LOG.warning(
                    "Jira ID is the same for commits, but commit message differs: \n"
                    f"Master branch commit: {mc.as_oneline_string()}\n"
                    f"Feature branch commit: {fc.as_oneline_string()}"
                )
                result_dict["matched_by_msg"].append((mc, fc))
        return result_dict


class RelatedCommits:
    def __init__(self):
        self.lst: List[RelatedCommitGroup] = []

    def add(self, cg: RelatedCommitGroup):
        self.lst.append(cg)


class SummaryData:
    def __init__(self, output_dir: str, run_legacy_script: bool, branch_data: Dict[BranchType, BranchData]):
        self.output_dir: str = output_dir
        self.run_legacy_script: bool = run_legacy_script
        self.branch_data: Dict[BranchType, BranchData] = branch_data
        self.merge_base: CommitData or None = None

        # Dict-based data structures, key: BranchType
        self.branch_names: Dict[BranchType, str] = {br_type: br_data.name for br_type, br_data in branch_data.items()}
        self.number_of_commits: Dict[BranchType, int] = {}
        self.all_commits_with_missing_jira_id: Dict[BranchType, List[CommitData]] = {}
        self.commits_with_missing_jira_id: Dict[BranchType, List[CommitData]] = {}

        # Inner-dict key: commit message, value: CommitData obj
        self.commits_with_missing_jira_id_filtered: Dict[BranchType, Dict] = {}
        self.unique_commits: Dict[BranchType, List[CommitData]] = {}

        # List-based data structures
        self.common_commits_before_merge_base: List[CommitData] = []

        # All common commits
        self.common_commits_after_merge_base: List[Tuple[CommitData, CommitData]] = []

        # Commits matched by message with missing Jira ID
        self.common_commits_matched_only_by_message: List[Tuple[CommitData, CommitData]] = []

        # Commits matched by Jira ID but not by message
        self.common_commits_matched_only_by_jira_id: List[Tuple[CommitData, CommitData]] = []

        # Commits matched by Jira ID and by message as well
        self.common_commits_matched_both: List[Tuple[CommitData, CommitData]] = []

        self.unique_jira_ids_legacy_script: Dict[BranchType, List[str]] = {}

        self.commit_groups: RelatedCommits = RelatedCommits()

    @property
    def common_commits(self):
        return [c[0] for c in self.common_commits_after_merge_base]

    @property
    def all_commits(self):
        all_commits: List[CommitData] = (
            [] + self.unique_commits[BranchType.MASTER] + self.unique_commits[BranchType.FEATURE] + self.common_commits
        )
        all_commits.sort(key=lambda c: c.date, reverse=True)
        return all_commits

    @property
    def all_commits_presence_matrix(self) -> List[List]:
        rows: List[List] = []
        for commit in self.all_commits:
            jira_id = commit.jira_id
            row: List[str] = [jira_id, commit.message, commit.date, commit.committer]

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

        res += "\n\n=====Stats: BRANCHES=====\n"
        for br_type, br_name in self.branch_names.items():
            res += f"Number of commits on {br_type.value} '{br_name}': {self.number_of_commits[br_type]}\n"

        res += "\n\n=====Stats: UNIQUE COMMITS=====\n"
        for br_type, br_name in self.branch_names.items():
            res += f"Number of unique commits on {br_type.value} '{br_name}': {len(self.unique_commits[br_type])}\n"

        if self.run_legacy_script:
            res += "\n\n=====Stats: UNIQUE COMMITS [LEGACY SCRIPT]=====\n"
            for br_type, br_name in self.branch_names.items():
                res += f"Number of unique commits on {br_type.value} '{br_name}': {len(self.unique_jira_ids_legacy_script[br_type])}\n"
        else:
            res += "\n\n=====Stats: UNIQUE COMMITS [LEGACY SCRIPT] - EXECUTION SKIPPED, NO DATA =====\n"

        res += "\n\n=====Stats: COMMON=====\n"
        res += f"Merge-base commit: {self.merge_base.hash} {self.merge_base.message} {self.merge_base.date}\n"
        res += f"Number of common commits before merge-base: {len(self.common_commits_before_merge_base)}\n"
        res += f"Number of common commits after merge-base: {len(self.common_commits_after_merge_base)}\n"

        for br_type, br_name in self.branch_names.items():
            res += f"\n\n=====Stats: COMMITS WITH MISSING JIRA ID ON BRANCH: {br_name}=====\n"
            res += (
                f"Number of all commits with missing Jira ID: {len(self.all_commits_with_missing_jira_id[br_type])}\n"
            )
            res += (
                f"Number of commits with missing Jira ID after merge-base: "
                f"{len(self.commits_with_missing_jira_id[br_type])}\n"
            )
            res += (
                f"Number of commits with missing Jira ID after merge-base, filtered by author exceptions: "
                f"{len(self.commits_with_missing_jira_id_filtered[br_type])}\n"
            )

        res += "\n\n=====Stats: COMMON COMMITS ACROSS BRANCHES=====\n"
        res += (
            f"Number of common commits with missing Jira ID, matched by commit message: "
            f"{len(self.common_commits_matched_only_by_message)}\n"
        )
        res += (
            f"Number of common commits with matching Jira ID but different commit message: "
            f"{len(self.common_commits_matched_only_by_jira_id)}\n"
        )
        res += (
            f"Number of common commits with matching Jira ID and commit message: "
            f"{len(self.common_commits_matched_both)}\n"
        )
        return res


class BranchComparatorConfig:
    def __init__(self, output_dir: str, args):
        self.output_dir = FileUtils.ensure_dir_created(
            FileUtils.join_path(output_dir, f"session-{DateUtils.now_formatted('%Y%m%d_%H%M%S')}")
        )
        self.commit_author_exceptions = args.commit_author_exceptions
        self.console_mode = True if "console_mode" in args and args.console_mode else False
        self.fail_on_missing_jira_id = False
        self.run_legacy_script = args.run_legacy_script
        self.legacy_compare_script_path = BranchComparatorConfig.find_git_compare_script()

    @staticmethod
    def find_git_compare_script():
        repo_root_dir = FileUtils.find_repo_root_dir(__file__, REPO_ROOT_DIRNAME)
        return FileUtils.join_path(repo_root_dir, "legacy-scripts", "branch-comparator", "git_compare.sh")


class Branches:
    def __init__(self, conf: BranchComparatorConfig, repo: GitWrapper, branch_dict: Dict[BranchType, str]):
        self.conf = conf
        self.repo = repo
        self.branch_data: Dict[BranchType, BranchData] = {}
        for br_type in BranchType:
            branch_name = branch_dict[br_type]
            self.branch_data[br_type] = BranchData(br_type, branch_name)
        self.fail_on_missing_jira_id = conf.fail_on_missing_jira_id

        # Set later
        self.merge_base: CommitData or None = None
        self.summary: SummaryData = SummaryData(self.conf.output_dir, self.conf.run_legacy_script, self.branch_data)

    def get_branch(self, br_type: BranchType) -> BranchData:
        return self.branch_data[br_type]

    @staticmethod
    def _generate_filename(basedir, prefix, branch_name="") -> str:
        return FileUtils.join_path(basedir, f"{prefix}{StringUtils.replace_special_chars(branch_name)}")

    def validate(self, br_type: BranchType):
        br_data = self.branch_data[br_type]
        branch_exist = self.repo.is_branch_exist(br_data.name)
        if not branch_exist:
            LOG.error(f"{br_data.type.name} does not exist with name '{br_data.name}'")
        return branch_exist

    def execute_git_log(self, print_stats=True, save_to_file=True):
        for br_type in BranchType:
            branch: BranchData = self.branch_data[br_type]
            branch.gitlog_results = self.repo.log(branch.name, oneline_with_date_author_committer=True)
            parse_config = GitLogParseConfig(
                log_format=GitLogLineFormat.ONELINE_WITH_DATE_AUTHOR_COMMITTER,
                pattern=ANY_JIRA_ID_PATTERN,
                allow_unmatched_jira_id=True,
                print_unique_jira_projects=True,
                jira_id_parse_strategy=MatchAllJiraIdStrategy(
                    type_preference=JiraIdTypePreference.UPSTREAM,
                    choose_preference=JiraIdChoosePreference.FIRST,
                    fallback_type=JiraIdTypePreference.DOWNSTREAM,
                ),
                keep_parser_state=True,
            )

            # Store commit objects in reverse order (ascending by date)
            branch.commit_objs = list(reversed(CommitData.from_git_log_output(branch.gitlog_results, parse_config)))
            self.summary.all_commits_with_missing_jira_id[br_type] = list(
                filter(lambda c: not c.jira_id, branch.commit_objs)
            )
            LOG.info(f"Found {len(self.summary.all_commits_with_missing_jira_id[br_type])} commits with empty Jira ID")

            LOG.debug(
                f"Found commits with empty Jira ID: {StringUtils.dict_to_multiline_string(self.summary.all_commits_with_missing_jira_id)}"
            )
            if self.fail_on_missing_jira_id:
                raise ValueError(
                    f"Found {len(self.summary.all_commits_with_missing_jira_id)} commits with empty Jira ID!"
                )

            for idx, commit in enumerate(branch.commit_objs):
                branch.hash_to_index[commit.hash] = idx
                if commit.jira_id not in branch.jira_id_to_commits:
                    branch.jira_id_to_commits[commit.jira_id] = []
                branch.jira_id_to_commits[commit.jira_id].append(commit)
        # This must be executed after branch.hash_to_index is set
        self.get_merge_base()

        self._record_stats_to_summary()
        if print_stats:
            self._print_stats()
        if save_to_file:
            self._write_git_log_to_file()

    def _record_stats_to_summary(self):
        for br_type in BranchType:
            branch: BranchData = self.branch_data[br_type]
            self.summary.number_of_commits[br_type] = branch.number_of_commits

    def _print_stats(self):
        for br_type in BranchType:
            branch: BranchData = self.branch_data[br_type]
            LOG.info(f"Found {branch.number_of_commits} commits on {br_type.value}: {branch.name}")

    def _write_git_log_to_file(self):
        for br_type in BranchType:
            branch: BranchData = self.branch_data[br_type]
            # We would like to maintain descending order of commits in printouts
            self.write_to_file_or_console("git log output full raw", branch, list(reversed(branch.commit_objs)))

    def _save_commits_before_after_merge_base_to_file(self):
        for br_type in BranchType:
            branch: BranchData = self.branch_data[br_type]
            self.write_to_file_or_console("before mergebase commits", branch, branch.commits_before_merge_base)
            self.write_to_file_or_console("after mergebase commits", branch, branch.commits_after_merge_base)

    def get_merge_base(self):
        merge_base: List[Commit] = self.repo.merge_base(
            self.branch_data[BranchType.FEATURE].name, self.branch_data[BranchType.MASTER].name
        )
        if len(merge_base) > 1:
            raise ValueError(f"Ambiguous merge base: {merge_base}.")
        elif len(merge_base) == 0:
            raise ValueError("Merge base not found between branches!")
        self.merge_base = CommitData.from_git_log_str(
            self.repo.log(
                merge_base[0].hexsha,
                oneline_with_date_author_committer=True,
            )[0],
            format=GitLogLineFormat.ONELINE_WITH_DATE_AUTHOR_COMMITTER,
            allow_unmatched_jira_id=True,
        )
        self.summary.merge_base = self.merge_base
        LOG.info(f"Merge base of branches: {self.merge_base}")
        for br_type in BranchType:
            branch: BranchData = self.branch_data[br_type]
            branch.set_merge_base(self.merge_base)

    def compare(self, commit_author_exceptions):
        self._save_commits_before_after_merge_base_to_file()
        feature_br: BranchData = self.branch_data[BranchType.FEATURE]
        master_br: BranchData = self.branch_data[BranchType.MASTER]

        self._sanity_check_commits_before_merge_base(feature_br, master_br)
        self._check_after_merge_base_commits(feature_br, master_br, commit_author_exceptions)

    def _sanity_check_commits_before_merge_base(self, feature_br: BranchData, master_br: BranchData):
        if len(master_br.commits_before_merge_base) != len(feature_br.commits_before_merge_base):
            raise ValueError(
                "Number of commits before merge_base does not match. "
                f"Feature branch has: {len(feature_br.commits_before_merge_base)} commits, "
                f"Master branch has: {len(master_br.commits_before_merge_base)} commits"
            )
        # Commit hashes up to the merge-base commit should be the same for both branches
        for idx, commit1 in enumerate(master_br.commits_before_merge_base):
            commit2 = feature_br.commits_before_merge_base[idx]
            if commit1.hash != commit2.hash:
                raise ValueError(
                    f"Commit hash mismatch below merge-base commit.\n"
                    f"Index: {idx}\n"
                    f"Hash of commit on {feature_br.name}: {commit2.hash}\n"
                    f"Hash of commit on {master_br.name}: {commit1.hash}"
                )
        self.summary.common_commits_before_merge_base = master_br.commits_before_merge_base
        LOG.info(
            f"Detected {len(self.summary.common_commits_before_merge_base)} common commits before merge-base between "
            f"'{feature_br.name}' and '{master_br.name}'"
        )

    def _check_after_merge_base_commits(
        self, feature_br: BranchData, master_br: BranchData, commit_author_exceptions: List[str]
    ):
        branches = [feature_br, master_br]
        self._print_all_jira_ids(branches)
        self._handle_commits_with_missing_jira_id(branches)
        self._handle_commits_with_missing_jira_id_filter_author(branches, commit_author_exceptions)

        common_jira_ids: Set[str] = set()
        common_commit_msgs: Set[str] = set()
        master_commits_by_message: Dict[str, CommitData] = self.summary.commits_with_missing_jira_id_filtered[
            BranchType.MASTER
        ]
        feature_commits_by_message: Dict[str, CommitData] = self.summary.commits_with_missing_jira_id_filtered[
            BranchType.FEATURE
        ]

        # List of tuples. First item: Master branch commit obj, second item: feature branch commit obj

        # 1. Go through commits on each branch, put commits into groups for the same jira
        # 2. Compare groups accross two branches
        # 3. Function: Get all ids of group
        # 4. Check if all commits are the same including the reverts
        # 5. If anything differs in groups, warn and write to file
        for master_commit in master_br.commits_after_merge_base:
            master_commit_msg = master_commit.message
            master_jira_id = master_commit.jira_id
            if not master_jira_id:
                # If this commit is without jira id and author was not an element of exceptional authors,
                # then try to match commits across branches by commit message.
                if master_commit_msg in master_commits_by_message:
                    LOG.debug(
                        "Trying to match commit by commit message as Jira ID is missing. Details: \n"
                        f"Branch: master branch\n"
                        f"Commit message: ${master_commit_msg}\n"
                    )
                    # Master commit message found in missing jira id list of the feature branch, record match
                    if master_commit_msg in feature_commits_by_message:
                        LOG.warning(
                            "Found matching commit by commit message. Details: \n"
                            f"Branch: master branch\n"
                            f"Commit message: ${master_commit_msg}\n"
                        )
                        common_commit_msgs.add(master_commit_msg)
                        commit_group: RelatedCommitGroup = RelatedCommitGroup(
                            [master_commit], [feature_commits_by_message[master_commit_msg]]
                        )
                        # ATM, these are groups that contain 1 master / 1 feature commit
                        self.summary.common_commits_after_merge_base.extend(commit_group.get_matched_by_msg)
                        self.summary.common_commits_matched_only_by_message.extend(commit_group.get_matched_by_msg)

            elif master_jira_id in feature_br.jira_id_to_commits:
                # Normal path: Try to match commits across branches by Jira ID
                feature_commits: List[CommitData] = feature_br.jira_id_to_commits[master_jira_id]
                LOG.debug(
                    "Found matching commits by jira id. Details: \n"
                    f"Master branch commit: {master_commit.as_oneline_string()}\n"
                    f"Feature branch commits: {[fc.as_oneline_string() for fc in feature_commits]}"
                )

                commit_group = RelatedCommitGroup([master_commit], feature_commits)
                self.summary.common_commits_matched_both.extend(commit_group.get_matched_by_id_and_msg)
                self.summary.common_commits_matched_only_by_jira_id.extend(commit_group.get_matched_by_id)
                # Either if commit message matched or not, count this as a common commit as Jira ID matched
                self.summary.common_commits_after_merge_base.extend(commit_group.get_matched_by_id)
                common_jira_ids.add(master_jira_id)

        self.write_commit_list_to_file_or_console(
            "commit message differs",
            self.summary.common_commits_matched_only_by_jira_id,
            add_sep_to_end=False,
            add_line_break_between_groups=True,
        )

        self.write_commit_list_to_file_or_console(
            "commits matched by message",
            self.summary.common_commits_matched_only_by_message,
            add_sep_to_end=False,
            add_line_break_between_groups=True,
        )

        master_br.unique_commits = self._filter_relevant_unique_commits(
            master_br.commits_after_merge_base,
            master_commits_by_message,
            common_jira_ids,
            common_commit_msgs,
        )
        feature_br.unique_commits = self._filter_relevant_unique_commits(
            feature_br.commits_after_merge_base,
            feature_commits_by_message,
            common_jira_ids,
            common_commit_msgs,
        )
        LOG.info(f"Identified {len(master_br.unique_commits)} unique commits on branch: {master_br.name}")
        LOG.info(f"Identified {len(feature_br.unique_commits)} unique commits on branch: {feature_br.name}")
        self.summary.unique_commits[BranchType.MASTER] = master_br.unique_commits
        self.summary.unique_commits[BranchType.FEATURE] = feature_br.unique_commits
        self.write_to_file_or_console("unique commits", master_br, master_br.unique_commits)
        self.write_to_file_or_console("unique commits", feature_br, feature_br.unique_commits)

    def _handle_commits_with_missing_jira_id_filter_author(self, branches: List[BranchData], commit_author_exceptions):
        # Create a dict of (commit message, CommitData),
        # filtering all the commits that has author from the exceptional authors.
        # Assumption: Commit message is unique for all commits
        for br_data in branches:
            self.summary.commits_with_missing_jira_id_filtered[br_data.type] = dict(
                [
                    (c.message, c)
                    for c in filter(
                        lambda c: c.author not in commit_author_exceptions,
                        self.summary.commits_with_missing_jira_id[br_data.type],
                    )
                ]
            )
            LOG.warning(
                f"Found {br_data.type.value} commits after merge-base with empty Jira ID "
                f"(after applied author filter: {commit_author_exceptions}): "
                f"{len(self.summary.commits_with_missing_jira_id_filtered[br_data.type])} "
            )
            LOG.debug(
                f"Found {br_data.type.value} commits after merge-base with empty Jira ID "
                f"(after applied author filter: {commit_author_exceptions}): "
                f"{StringUtils.list_to_multiline_string(self.summary.commits_with_missing_jira_id_filtered[br_data.type])}"
            )
        for br_data in branches:
            self.write_to_file_or_console(
                "commits missing jira id filtered", br_data, self.summary.commits_with_missing_jira_id[br_data.type]
            )

    def _handle_commits_with_missing_jira_id(self, branches: List[BranchData]):
        # TODO write these to file
        # TODO also write commits with multiple jira IDs
        for br_data in branches:
            self.summary.commits_with_missing_jira_id[br_data.type]: List[CommitData] = list(
                filter(lambda c: not c.jira_id, br_data.commits_after_merge_base)
            )

            LOG.warning(
                f"Found {br_data.type.value} "
                f"commits after merge-base with empty Jira ID: "
                f"{len(self.summary.commits_with_missing_jira_id[br_data.type])}"
            )
            LOG.debug(
                f"Found {br_data.type.value} "
                f"commits after merge-base with empty Jira ID: "
                f"{StringUtils.list_to_multiline_string(self.summary.commits_with_missing_jira_id[br_data.type])}"
            )
        for br_data in branches:
            self.write_to_file_or_console(
                "commits missing jira id", br_data, self.summary.commits_with_missing_jira_id[br_data.type]
            )

    @staticmethod
    def _filter_relevant_unique_commits(
        commits: List[CommitData], commits_by_message: Dict[str, CommitData], common_jira_ids, common_commit_msgs
    ) -> List[CommitData]:
        result = []
        # 1. Values of commit list can contain commits without Jira ID
        # and we don't want to count them as unique commits unless the commit is a
        # special authored commit and it's not a common commit by its message
        # 2. If Jira ID is in common_jira_ids, it's not a unique commit, either.
        for commit in commits:
            special_unique_commit = (
                not commit.jira_id and commit.message in commits_by_message and commit.message not in common_commit_msgs
            )
            normal_unique_commit = commit.jira_id is not None and commit.jira_id not in common_jira_ids
            if special_unique_commit or normal_unique_commit:
                result.append(commit)
        return result

    def write_to_file_or_console(self, output_type: str, branch: BranchData, commits: List[CommitData]):
        contents = StringUtils.list_to_multiline_string([self.convert_commit_to_str(c) for c in commits])
        if self.conf.console_mode:
            LOG.info(f"Printing {output_type} for branch {branch.type.name}: {contents}")
        else:
            fn_prefix = Branches._convert_output_type_str_to_file_prefix(output_type)
            f = self._generate_filename(self.conf.output_dir, fn_prefix, branch.shortname)
            LOG.info(f"Saving {output_type} for branch {branch.type.name} to file: {f}")
            FileUtils.save_to_file(f, contents)

    def write_commit_list_to_file_or_console(
        self,
        output_type: str,
        commit_groups: List[Tuple[CommitData, CommitData]],
        add_sep_to_end=True,
        add_line_break_between_groups=False,
    ):
        if not add_line_break_between_groups:
            commits = [self.convert_commit_to_str(commit) for tup in commit_groups for commit in tup]
            contents = StringUtils.list_to_multiline_string(commits)
        else:
            contents = ""
            for tup in commit_groups:
                commit_strs = [self.convert_commit_to_str(commit) for commit in tup]
                contents += StringUtils.list_to_multiline_string(commit_strs)
                contents += "\n\n"

        if self.conf.console_mode:
            LOG.info(f"Printing {output_type}: {contents}")
        else:
            fn_prefix = Branches._convert_output_type_str_to_file_prefix(output_type, add_sep_to_end=add_sep_to_end)
            f = self._generate_filename(self.conf.output_dir, fn_prefix)
            LOG.info(f"Saving {output_type} to file: {f}")
            FileUtils.save_to_file(f, contents)

    @staticmethod
    def _convert_output_type_str_to_file_prefix(output_type, add_sep_to_end=True):
        file_prefix: str = output_type.replace(" ", "-")
        if add_sep_to_end:
            file_prefix += "-"
        return file_prefix

    def _print_all_jira_ids(self, branches: List[BranchData]):
        for br_data in branches:
            LOG.info(f"Printing jira IDs for {br_data.type.value}...")
            for c in br_data.commits_after_merge_base:
                LOG.info(f"Jira ID: {c.jira_id}, commit message: {c.message}")

    @staticmethod
    def convert_commit_to_str(commit: CommitData):
        return commit.as_oneline_string(incl_date=True, incl_author=False, incl_committer=True)


# TODO Handle multiple jira ids?? example: "CDPD-10052. HADOOP-16932"
# TODO Consider revert commits?
# TODO Add documentation
# TODO Check in logs: all results for "Jira ID is the same for commits, but commit message differs"


class LegacyScriptRunner:
    @staticmethod
    def start(config, branches, repo_path):
        script_results: Dict[BranchType, Tuple[str, str]] = LegacyScriptRunner._execute_compare_script(
            config, branches, working_dir=repo_path
        )
        for br_type in BranchType:
            branches.summary.unique_jira_ids_legacy_script[
                br_type
            ] = LegacyScriptRunner._get_unique_jira_ids_for_branch(script_results, branches.get_branch(br_type))
            LOG.debug(
                f"[LEGACY SCRIPT] Unique commit results for {br_type.value}: "
                f"{branches.summary.unique_jira_ids_legacy_script[br_type]}"
            )
        # Cross check unique jira ids with previous results
        for br_type in BranchType:
            branch_data = branches.get_branch(br_type)
            unique_jira_ids = [c.jira_id for c in branches.summary.unique_commits[br_type]]
            LOG.info(f"[CURRENT SCRIPT] Found {len(unique_jira_ids)} unique commits on {br_type} '{branch_data.name}'")
            LOG.debug(f"[CURRENT SCRIPT] Found unique commits on {br_type} '{branch_data.name}': {unique_jira_ids} ")

    @staticmethod
    def _get_unique_jira_ids_for_branch(script_results: Dict[BranchType, Tuple[str, str]], branch_data: BranchData):
        branch_type = branch_data.type
        res_tuple = script_results[branch_type]
        LOG.info(f"CLI Command for {branch_type} was: {res_tuple[0]}")
        LOG.info(f"Output of command for {branch_type} was: {res_tuple[1]}")
        lines = res_tuple[1].splitlines()
        unique_jira_ids = [line.split(" ")[0] for line in lines]
        LOG.info(f"[LEGACY SCRIPT] Found {len(unique_jira_ids)} unique commits on {branch_type} '{branch_data.name}'")
        LOG.debug(f"[LEGACY SCRIPT] Found unique commits on {branch_type} '{branch_data.name}': {unique_jira_ids}")
        return unique_jira_ids

    @staticmethod
    def _execute_compare_script(config, branches, working_dir) -> Dict[BranchType, Tuple[str, str]]:
        compare_script = config.legacy_compare_script_path
        master_br_name = branches.get_branch(BranchType.MASTER).shortname
        feature_br_name = branches.get_branch(BranchType.FEATURE).shortname
        output_dir = FileUtils.join_path(config.output_dir, "git_compare_script_output")
        FileUtils.ensure_dir_created(output_dir)

        results: Dict[BranchType, Tuple[str, str]] = {
            BranchType.MASTER: LegacyScriptRunner._exec_script_only_on_master(
                compare_script, feature_br_name, master_br_name, output_dir, working_dir
            ),
            BranchType.FEATURE: LegacyScriptRunner._exec_script_only_on_feature(
                compare_script, feature_br_name, master_br_name, output_dir, working_dir
            ),
        }
        return results

    @staticmethod
    def _exec_script_only_on_master(compare_script, feature_br_name, master_br_name, output_dir, working_dir):
        args1 = f"{feature_br_name} {master_br_name}"
        output_file1 = FileUtils.join_path(output_dir, f"only-on-{master_br_name}")
        cli_cmd, cli_output = CommandRunner.execute_script(
            compare_script, args=args1, working_dir=working_dir, output_file=output_file1, use_tee=True
        )
        return cli_cmd, cli_output

    @staticmethod
    def _exec_script_only_on_feature(compare_script, feature_br_name, master_br_name, output_dir, working_dir):
        args2 = f"{master_br_name} {feature_br_name}"
        output_file2 = FileUtils.join_path(output_dir, f"only-on-{feature_br_name}")
        cli_cmd, cli_output = CommandRunner.execute_script(
            compare_script, args=args2, working_dir=working_dir, output_file=output_file2, use_tee=True
        )
        return cli_cmd, cli_output


class BranchComparator:
    """"""

    def __init__(self, args, downstream_repo, output_dir: str):
        self.repo = downstream_repo
        self.config = BranchComparatorConfig(output_dir, args)
        self.branches: Branches = Branches(
            self.config, self.repo, {BranchType.FEATURE: args.feature_branch, BranchType.MASTER: args.master_branch}
        )

    def run(self):
        LOG.info(
            "Starting Branch comparator... \n "
            f"Output dir: {self.config.output_dir}\n"
            f"Master branch: {self.branches.get_branch(BranchType.MASTER).name}\n "
            f"Feature branch: {self.branches.get_branch(BranchType.FEATURE).name}\n "
            f"Commit author exceptions: {self.config.commit_author_exceptions}\n "
            f"Console mode: {self.config.console_mode}\n "
            f"Run legacy comparator script: {self.config.run_legacy_script}\n "
        )
        self.validate_branches()
        # TODO Make fetching optional, argparse argument
        # self.repo.fetch(all=True)
        print_stats = self.config.console_mode
        save_to_file = not self.config.console_mode
        self.compare(print_stats=print_stats, save_to_file=save_to_file)
        if self.config.run_legacy_script:
            LegacyScriptRunner.start(self.config, self.branches, self.repo.repo_path)
        self.print_and_save_summary()

    def validate_branches(self):
        both_exist = self.branches.validate(BranchType.FEATURE)
        both_exist &= self.branches.validate(BranchType.MASTER)
        if not both_exist:
            raise ValueError("Both feature and master branch should be an existing branch. Exiting...")

    def compare(self, print_stats=True, save_to_file=True):
        self.branches.execute_git_log(print_stats=print_stats, save_to_file=save_to_file)
        self.branches.compare(self.config.commit_author_exceptions)

    def print_and_save_summary(self):
        rendered_sum = RenderedSummary.from_summary_data(self.branches.summary)
        LOG.info(rendered_sum.printable_summary_str)

        filename = FileUtils.join_path(self.config.output_dir, SUMMARY_FILE_TXT)
        LOG.info(f"Saving summary to text file: {filename}")
        FileUtils.save_to_file(filename, rendered_sum.writable_summary_str)

        filename = FileUtils.join_path(self.config.output_dir, SUMMARY_FILE_HTML)
        LOG.info(f"Saving summary to html file: {filename}")
        FileUtils.save_to_file(filename, rendered_sum.html_summary)


class TableWithHeader:
    def __init__(
        self, header_title, table: str, table_fmt: TabulateTableFormat, colorized: bool = False, branch: str = None
    ):
        self.header = (
            StringUtils.generate_header_line(
                header_title, char="═", length=len(StringUtils.get_first_line_of_multiline_str(table))
            )
            + "\n"
        )
        self.table = table
        self.table_fmt: TabulateTableFormat = table_fmt
        self.colorized = colorized
        self.branch = branch

    @property
    def is_branch_based(self):
        return self.branch is not None

    def __str__(self):
        return self.header + self.table


class RenderedTableType(Enum):
    RESULT_FILES = ("result_files", "RESULT FILES")
    UNIQUE_ON_BRANCH = ("unique_on_branch", "UNIQUE ON BRANCH $$")
    COMMON_COMMITS_SINCE_DIVERGENCE = ("common_commits_since_divergence", "COMMON COMMITS SINCE BRANCHES DIVERGED")
    ALL_COMMITS_MERGED = ("all_commits_merged", "ALL COMMITS (MERGED LIST)")

    def __init__(self, key, header_value):
        self.key = key
        self.header = header_value


class RenderedSummary:
    """
    Properties of tables: Table format, RenderedTableType, Branch, Colorized or not.
    - Table format: Normal (regular) / HTML / Any future formats.
    - RenderedTableType: all values of enum.
    - Branch: Some tables are branch-based, e.g. RenderedTableType.UNIQUE_ON_BRANCH
    - Colorized: Bool value indicating if the table values are colorized
    """

    def __init__(self, summary_data: SummaryData):
        self._tables: Dict[RenderedTableType, List[TableWithHeader]] = {}
        self._tables_with_branch: Dict[RenderedTableType, bool] = {RenderedTableType.UNIQUE_ON_BRANCH: True}
        self.summary_data = summary_data
        self.add_result_files_table()
        self.add_unique_commit_tables()
        self.add_common_commits_table()
        self.add_all_commits_tables()
        self.summary_str = self.generate_summary_string()
        self.printable_summary_str, self.writable_summary_str, self.html_summary = self.generate_summary_msgs()

    def add_table(self, ttype: RenderedTableType, table: TableWithHeader):
        if table.is_branch_based and ttype not in self._tables_with_branch:
            raise ValueError(
                f"Unexpected table type for branch-based table: {ttype}. "
                f"Possible table types with branch info: {[k.name for k in self._tables_with_branch.keys()]}"
            )
        if ttype not in self._tables:
            self._tables[ttype] = []
        self._tables[ttype].append(table)

    def get_tables(
        self,
        ttype: RenderedTableType,
        colorized: bool = False,
        table_fmt: TabulateTableFormat = TabulateTableFormat.GRID,
        branch: str = None,
    ):
        tables = self._tables[ttype]
        result: List[TableWithHeader] = []
        for table in tables:
            # TODO simplify with filter
            if table.colorized == colorized and table.table_fmt == table_fmt and table.branch == branch:
                result.append(table)
        return result

    def get_branch_based_tables(self, ttype: RenderedTableType, table_fmt: TabulateTableFormat):
        tables: List[TableWithHeader] = []
        for br_type, br_data in self.summary_data.branch_data.items():
            tables.extend(self.get_tables(ttype, colorized=False, table_fmt=table_fmt, branch=br_data.name))
        return tables

    @staticmethod
    def from_summary_data(summary_data: SummaryData):
        return RenderedSummary(summary_data)

    def add_result_files_table(self):
        result_files_data = sorted(
            FileUtils.find_files(self.summary_data.output_dir, regex=".*", full_path_result=True)
        )
        table_type = RenderedTableType.RESULT_FILES
        gen_tables = ResultPrinter.print_tables(
            result_files_data,
            lambda file: (file, len(FileUtils.read_file(file).splitlines())),
            header=[HEADER_ROW, HEADER_FILE, HEADER_NO_OF_LINES],
            print_result=False,
            max_width=200,
            max_width_separator=os.sep,
            tabulate_fmts=DEFAULT_TABLE_FORMATS,
        )

        for table_fmt, table in gen_tables.items():
            self.add_table(
                table_type, TableWithHeader(table_type.header, table, table_fmt=table_fmt, colorized=False, branch=None)
            )

    def add_unique_commit_tables(self):
        table_type = RenderedTableType.UNIQUE_ON_BRANCH
        for br_type, br_data in self.summary_data.branch_data.items():
            header_value = table_type.header.replace("$$", br_data.name)
            gen_tables = ResultPrinter.print_tables(
                self.summary_data.unique_commits[br_type],
                lambda commit: (commit.jira_id, commit.message, commit.date, commit.committer),
                header=[HEADER_ROW, HEADER_JIRA_ID, HEADER_COMMIT_MSG, HEADER_COMMIT_DATE, HEADER_COMMITTER],
                print_result=False,
                max_width=80,
                max_width_separator=" ",
                tabulate_fmts=DEFAULT_TABLE_FORMATS,
            )
            for table_fmt, table in gen_tables.items():
                self.add_table(
                    table_type,
                    TableWithHeader(header_value, table, table_fmt=table_fmt, colorized=False, branch=br_data.name),
                )

    def add_common_commits_table(self):
        table_type = RenderedTableType.COMMON_COMMITS_SINCE_DIVERGENCE
        gen_tables = ResultPrinter.print_tables(
            self.summary_data.common_commits,
            lambda commit: (commit.jira_id, commit.message, commit.date, commit.committer),
            header=[HEADER_ROW, HEADER_JIRA_ID, HEADER_COMMIT_MSG, HEADER_COMMIT_DATE, HEADER_COMMITTER],
            print_result=False,
            max_width=80,
            max_width_separator=" ",
            tabulate_fmts=DEFAULT_TABLE_FORMATS,
        )
        for table_fmt, table in gen_tables.items():
            self.add_table(table_type, TableWithHeader(table_type.header, table, table_fmt=table_fmt, colorized=False))

    def add_all_commits_tables(self):
        all_commits: List[List] = self.summary_data.all_commits_presence_matrix

        header = [HEADER_ROW, HEADER_JIRA_ID, HEADER_COMMIT_MSG, HEADER_COMMIT_DATE, HEADER_COMMITTER]
        header.extend(self.summary_data.get_branch_names())

        # Adding 1 because row id will be added as first column
        row_len = len(all_commits[0]) + 1
        color_conf = ColorizeConfig(
            [
                ColorDescriptor(bool, True, Color.GREEN, MatchType.ALL, (0, row_len), (0, row_len)),
                ColorDescriptor(bool, False, Color.RED, MatchType.ANY, (0, row_len), (0, row_len)),
            ],
            eval_method=EvaluationMethod.ALL,
        )
        self._add_all_comits_table(header, all_commits, colorize_conf=color_conf)
        self._add_all_comits_table(header, all_commits, colorize_conf=None)

    def _add_all_comits_table(self, header, all_commits, colorize_conf: ColorizeConfig = None):
        table_type = RenderedTableType.ALL_COMMITS_MERGED
        colorize = True if colorize_conf else False
        gen_tables = ResultPrinter.print_tables(
            all_commits,
            lambda row: row,
            header=header,
            print_result=False,
            max_width=100,
            max_width_separator=" ",
            bool_conversion_config=BoolConversionConfig(),
            colorize_config=colorize_conf,
        )

        for table_fmt, table in gen_tables.items():
            self.add_table(
                table_type, TableWithHeader(table_type.header, table, table_fmt=table_fmt, colorized=colorize)
            )

    def generate_summary_string(self):
        a_normal_table = self.get_tables(
            RenderedTableType.COMMON_COMMITS_SINCE_DIVERGENCE, table_fmt=TabulateTableFormat.GRID
        )[0]
        length_of_table_first_line = StringUtils.get_first_line_of_multiline_str(a_normal_table.table)
        summary_str = "\n\n" + (
            StringUtils.generate_header_line("SUMMARY", char="═", length=len(length_of_table_first_line)) + "\n"
        )
        summary_str += str(self.summary_data) + "\n\n"
        return summary_str

    def generate_summary_msgs(self):
        def regular_table(table_type: RenderedTableType):
            return self.get_tables(table_type, colorized=False, table_fmt=TabulateTableFormat.GRID, branch=None)

        def get_branch_tables(table_type: RenderedTableType):
            return self.get_branch_based_tables(table_type, table_fmt=TabulateTableFormat.GRID)

        def html_table(table_type: RenderedTableType):
            return self.get_tables(table_type, colorized=False, table_fmt=TabulateTableFormat.HTML, branch=None)

        def get_html_branch_tables(table_type: RenderedTableType):
            return self.get_branch_based_tables(table_type, table_fmt=TabulateTableFormat.HTML)

        def get_colorized_tables(table_type: RenderedTableType, colorized=False):
            return self.get_tables(table_type, table_fmt=TabulateTableFormat.GRID, colorized=colorized, branch=None)

        rt = regular_table
        ht = html_table
        rtt = RenderedTableType
        bt = get_branch_tables
        hbt = get_html_branch_tables
        cbt = get_colorized_tables

        printable_tables: List[TableWithHeader] = (
            rt(rtt.RESULT_FILES)
            + bt(rtt.UNIQUE_ON_BRANCH)
            + rt(rtt.COMMON_COMMITS_SINCE_DIVERGENCE)
            + cbt(rtt.ALL_COMMITS_MERGED, colorized=True)
        )
        writable_tables: List[TableWithHeader] = (
            rt(rtt.RESULT_FILES)
            + bt(rtt.UNIQUE_ON_BRANCH)
            + rt(rtt.COMMON_COMMITS_SINCE_DIVERGENCE)
            + cbt(rtt.ALL_COMMITS_MERGED, colorized=False)
        )

        html_tables: List[TableWithHeader] = (
            ht(rtt.RESULT_FILES)
            + hbt(rtt.UNIQUE_ON_BRANCH)
            + ht(rtt.COMMON_COMMITS_SINCE_DIVERGENCE)
            + ht(rtt.ALL_COMMITS_MERGED)
        )
        return (
            self._generate_summary_str(printable_tables),
            self._generate_summary_str(writable_tables),
            self.generate_summary_html(html_tables),
        )

    def _generate_summary_str(self, tables):
        printable_summary_str: str = self.summary_str
        for table in tables:
            printable_summary_str += str(table)
            printable_summary_str += "\n\n"
        return printable_summary_str

    def generate_summary_html(self, html_tables, separator_tag="hr", add_breaks=2) -> str:
        html_sep = f"<{separator_tag}/>"
        html_sep += add_breaks * "<br/>"
        tables_html: str = html_sep
        tables_html += html_sep.join([f"<h1>{h.header}</h1>" + h.table for h in html_tables])

        soup = BeautifulSoup()
        self._add_summary_as_html_paragraphs(soup)
        html = soup.new_tag("html")
        self._add_html_head_and_style(soup, html)
        html.append(BeautifulSoup(tables_html, "html.parser"))
        soup.append(html)
        return soup.prettify()

    def _add_summary_as_html_paragraphs(self, soup):
        lines = self.summary_str.splitlines()
        for line in lines:
            p = soup.new_tag("p")
            p.append(line)
            soup.append(p)

    def _add_html_head_and_style(self, soup, html):
        head = soup.new_tag("head")
        style = soup.new_tag("style")
        style.string = """
table, th, td {
  border: 1px solid black;
}
"""
        head.append(style)
        html.append(head)
