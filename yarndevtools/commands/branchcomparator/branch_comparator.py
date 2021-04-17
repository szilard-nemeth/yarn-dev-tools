import logging
from enum import Enum
from typing import Dict, List

from git import Commit
from pythoncommons.collection_utils import CollectionUtils
from pythoncommons.date_utils import DateUtils
from pythoncommons.file_utils import FileUtils
from pythoncommons.git_wrapper import GitWrapper

from yarndevtools.commands.branchcomparator.common import (
    BranchType,
    BranchData,
)
from yarndevtools.commands.branchcomparator.common_representation import SummaryDataAbs, OutputManager
from yarndevtools.commands.branchcomparator.group_matching import GroupedCommitMatcher
from yarndevtools.commands.branchcomparator.legacy_script import LegacyScriptRunner
from yarndevtools.commands.branchcomparator.simple_matching import SimpleCommitMatcher
from yarndevtools.commands_common import (
    CommitData,
    GitLogLineFormat,
    GitLogParseConfig,
    MatchAllJiraIdStrategy,
    JiraIdTypePreference,
    JiraIdChoosePreference,
)
from yarndevtools.constants import ANY_JIRA_ID_PATTERN, REPO_ROOT_DIRNAME

LOG = logging.getLogger(__name__)


class CommitMatchingAlgorithm(Enum):
    SIMPLE = SimpleCommitMatcher
    GROUPED = GroupedCommitMatcher


class BranchComparatorConfig:
    def __init__(self, output_dir: str, args):
        self.output_dir = FileUtils.ensure_dir_created(
            FileUtils.join_path(output_dir, f"session-{DateUtils.now_formatted('%Y%m%d_%H%M%S')}")
        )
        self.commit_author_exceptions = args.commit_author_exceptions
        self.console_mode = True if "console_mode" in args and args.console_mode else False
        self.save_to_file = not self.console_mode
        self.fail_on_missing_jira_id = False
        self.run_legacy_script = args.run_legacy_script
        self.legacy_compare_script_path = BranchComparatorConfig.find_git_compare_script()

        # TODO make this object instance configurable
        self.matching_algorithm = CommitMatchingAlgorithm.GROUPED
        # self.matching_algorithm = CommitMatchingAlgorithm.SIMPLE
        self.output_manager = OutputManager(self)

    @staticmethod
    def find_git_compare_script():
        repo_root_dir = FileUtils.find_repo_root_dir(__file__, REPO_ROOT_DIRNAME)
        return FileUtils.join_path(repo_root_dir, "legacy-scripts", "branch-comparator", "git_compare.sh")


class Branches:
    def __init__(self, conf: BranchComparatorConfig, repo: GitWrapper, branch_dict: Dict[BranchType, str]):
        self.all_commits_with_missing_jira_id: Dict[BranchType, List[CommitData]] = {}
        self.config = conf
        self.repo = repo
        self.branch_data: Dict[BranchType, BranchData] = {}
        for br_type in BranchType:
            branch_name = branch_dict[br_type]
            self.branch_data[br_type] = BranchData(br_type, branch_name)

        if self.config.matching_algorithm == CommitMatchingAlgorithm.SIMPLE:
            self.commit_matcher = SimpleCommitMatcher(self.branch_data)
        elif self.config.matching_algorithm == CommitMatchingAlgorithm.GROUPED:
            self.commit_matcher = GroupedCommitMatcher(self.branch_data)

        # These are set later
        self.merge_base: CommitData or None = None

    def get_branch(self, br_type: BranchType) -> BranchData:
        return self.branch_data[br_type]

    def validate(self, br_type: BranchType):
        br_data = self.branch_data[br_type]
        branch_exist = self.repo.is_branch_exist(br_data.name)
        if not branch_exist:
            LOG.error(f"{br_data.type.name} does not exist with name '{br_data.name}'")
        return branch_exist

    def execute_git_log(self):
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
            branch.set_commit_objs(list(reversed(CommitData.from_git_log_output(branch.gitlog_results, parse_config))))
            for idx, commit in enumerate(branch.commit_objs):
                branch.hash_to_index[commit.hash] = idx
                # TODO add a special key like "NO_JIRA_ID" that groups all commits without jira id, right now these are overwriting
                #  because key will be None
                if commit.jira_id not in branch.jira_id_to_commits:
                    branch.jira_id_to_commits[commit.jira_id] = []
                branch.jira_id_to_commits[commit.jira_id].append(commit)

        # These must be executed after branch.hash_to_index is set !
        self.set_commits_with_missing_jira_id()
        self.get_merge_base()
        for br_type in BranchType:
            branch: BranchData = self.branch_data[br_type]
            branch.commits_after_merge_base_filtered = list(
                filter(lambda c: c.author not in self.config.commit_author_exceptions, branch.commits_after_merge_base)
            )

    def pre_compare(self):
        feature_br: BranchData = self.branch_data[BranchType.FEATURE]
        master_br: BranchData = self.branch_data[BranchType.MASTER]
        branches = [feature_br, master_br]
        self._sanity_check_commits_before_merge_base(feature_br, master_br)
        self._determine_commits_with_missing_jira_id(branches)

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
        for br_type in BranchType:
            branch: BranchData = self.branch_data[br_type]
            branch.set_merge_base(self.merge_base)

    def compare(self) -> SummaryDataAbs:
        # TODO Make the 2 commit matchers have the same interface somehow (?)
        if self.config.matching_algorithm == CommitMatchingAlgorithm.SIMPLE:
            # At this point, sanity check verified commits before merge-base,
            # we can set it from any of master / feature branch
            common_commits = self.commit_matcher.create_common_commits_obj()
            common_commits.before_merge_base = self.branch_data[BranchType.MASTER].commits_before_merge_base
            self.config.output_manager.print_or_write_to_file_before_compare(
                self.branch_data, self.merge_base, common_commits
            )

            # Let the game begin :)
            # Start to compare / A.K.A. match commits
            # TODO move these to compare match_commits method, _write_commit_match_result_files also implementation specific
            self.commit_matcher.match_commits()
            summary: SummaryDataAbs = self.commit_matcher.create_summary_data(self.config, self, common_commits)
            self.config.output_manager.write_commit_match_result_files(self.branch_data, common_commits)
        elif self.config.matching_algorithm == CommitMatchingAlgorithm.GROUPED:
            self.commit_matcher.match_commits()
            return None
            # TODO call the rest of the required methods + return with something
        return summary

    @staticmethod
    def _sanity_check_commits_before_merge_base(feature_br: BranchData, master_br: BranchData):
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

    def _determine_commits_with_missing_jira_id(self, branches: List[BranchData]):
        # If fail on missing jira id is configured, fail-fast
        if self.config.fail_on_missing_jira_id:
            len_of_all_lists = CollectionUtils.sum_len_of_lists_in_dict(self.all_commits_with_missing_jira_id)
            raise ValueError(f"Found {len_of_all_lists} commits with missing Jira ID! " f"Halting as configured")
        # TODO write commits with multiple jira IDs to a file
        for br_data in branches:
            br_data.commits_with_missing_jira_id = list(
                filter(lambda c: not c.jira_id, br_data.commits_after_merge_base)
            )
            # Create a dict of (commit message, CommitData),
            # filtering all the commits that has author from the authors to filter.
            # IMPORTANT Assumption: Commit message is unique for all commits --> This is bad
            # Example:
            # CommitData(hash=45fa9a281222a600f056ac40d1e2edddce029186, jira_id=None, message=SPNEGO TLS verification,
            #     date=2020-05-05T09:58:45-07:00, branches=None, reverted=False, author=eyang@apache.org,
            #     committer=weichiu@cloudera.com, reverted_at_least_once=False,
            #     jira_id_data=JiraIdData(chosen=None, _all_matched={}))
            # CommitData(hash=199768ddb9cbc6e10234dec390f7f1ec03445df7, jira_id=None, message=SPNEGO TLS verification,
            #     date=2020-10-21T12:02:22-07:00, branches=None, reverted=False, author=eyang@apache.org,
            #     committer=weichiu@cloudera.com, reverted_at_least_once=False,
            #     jira_id_data=JiraIdData(chosen=None, _all_matched={}))
            br_data.commits_with_missing_jira_id_filtered = dict(
                [
                    (c.hash, c)
                    for c in filter(
                        lambda c: c.author not in self.config.commit_author_exceptions,
                        br_data.commits_with_missing_jira_id,
                    )
                ]
            )

    def set_commits_with_missing_jira_id(self):
        for br_type, br_data in self.branch_data.items():
            self.all_commits_with_missing_jira_id[br_type] = br_data.all_commits_with_missing_jira_id


# TODO Add documentation
class BranchComparator:
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
        summary_data = self.compare()
        if self.config.run_legacy_script:
            LegacyScriptRunner.start(self.config, self.branches, self.repo.repo_path)
        self.config.output_manager.print_and_save_summary(summary_data)

    def validate_branches(self):
        both_exist = self.branches.validate(BranchType.FEATURE)
        both_exist &= self.branches.validate(BranchType.MASTER)
        if not both_exist:
            raise ValueError("Both feature and master branch should be an existing branch. Exiting...")

    def compare(self) -> SummaryDataAbs:
        self.branches.execute_git_log()
        self.branches.pre_compare()
        return self.branches.compare()
