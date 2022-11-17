import logging
from enum import Enum
from typing import Dict, List

from git import Commit
from pythoncommons.object_utils import CollectionUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils
from pythoncommons.file_utils import FileUtils
from pythoncommons.git_wrapper import GitWrapper, GitLogLineFormat

from yarndevtools.commands.branchcomparator.common import (
    BranchType,
    BranchData,
    CommitMatcherBase,
    MatchingResultBase,
)
from yarndevtools.commands.branchcomparator.group_matching import (
    GroupedCommitMatcher,
    GroupedOutputManager,
)
from yarndevtools.commands.branchcomparator.legacy_script import LegacyScriptRunner
from yarndevtools.commands.branchcomparator.simple_matching import (
    SimpleCommitMatcher,
    SimpleOutputManager,
)
from yarndevtools.commands_common import (
    CommitData,
    GitLogParseConfig,
    MatchAllJiraIdStrategy,
    JiraIdTypePreference,
    JiraIdChoosePreference,
    CommandAbs,
)
from yarndevtools.constants import ANY_JIRA_ID_PATTERN, REPO_ROOT_DIRNAME, YARNDEVTOOLS_MODULE_NAME
from yarndevtools.common.shared_command_utils import CommandType, RepoType
from yarndevtools.yarn_dev_tools_config import YarnDevToolsConfig

LOG = logging.getLogger(__name__)


class CommitMatchingAlgorithm(Enum):
    SIMPLE = ("simple", SimpleCommitMatcher, SimpleOutputManager)
    GROUPED = ("grouped", GroupedCommitMatcher, GroupedOutputManager)

    def __init__(self, name, matcher_class, om_class):
        self.shortname = name
        self.matcher_class = matcher_class
        self.output_manager_class = om_class

    def __str__(self):
        return self.shortname

    def __repr__(self):
        return str(self)

    @staticmethod
    def valid_values():
        return {e.shortname: e for e in CommitMatchingAlgorithm}

    @staticmethod
    def argparse(s):
        try:
            return CommitMatchingAlgorithm[s.upper()]
        except KeyError:
            return s


class BranchComparatorConfig:
    def __init__(self, output_dir: str, args, branch_names: Dict[BranchType, str]):
        self.output_dir = ProjectUtils.get_session_dir_under_child_dir(FileUtils.basename(output_dir))
        self.commit_author_exceptions = args.commit_author_exceptions
        self.console_mode = True if "console_mode" in args and args.console_mode else False
        self.save_to_file = not self.console_mode
        self.fail_on_missing_jira_id = False
        self.run_legacy_script = args.run_legacy_script
        self.legacy_compare_script_path = BranchComparatorConfig.find_git_compare_script()
        self.matching_algorithm: CommitMatchingAlgorithm = args.algorithm
        self.branch_names = branch_names
        self.repo_type: RepoType = (
            RepoType[args.repo_type.upper()] if hasattr(args, "repo_type") else RepoType.DOWNSTREAM
        )
        self.full_cmd: str or None = None

    def __str__(self):
        return (
            f"Full command was: {self.full_cmd} \n"
            f"Matching algorithm / class: {self.matching_algorithm} / "
            f"{self.matching_algorithm.matcher_class.__name__} \n"
            f"Output dir: {self.output_dir} \n"
            f"Repo type: {self.repo_type} \n"
            f"Master branch: {self.branch_names[BranchType.MASTER]}\n"
            f"Feature branch: {self.branch_names[BranchType.FEATURE]}\n"
            f"Commit author exceptions: {self.commit_author_exceptions}\n"
            f"Console mode: {self.console_mode}\n"
            f"Run legacy comparator script: {self.run_legacy_script}\n"
        )

    @staticmethod
    def find_git_compare_script():
        basedir = FileUtils.find_repo_root_dir(__file__, REPO_ROOT_DIRNAME, raise_error=False)
        if not basedir:
            basedir = FileUtils.find_repo_root_dir(__file__, YARNDEVTOOLS_MODULE_NAME, raise_error=True)
        return FileUtils.join_path(basedir, "legacy-scripts", "branch-comparator", "git_compare.sh")


class Branches:
    def __init__(self, conf: BranchComparatorConfig, repo: GitWrapper, branch_dict: Dict[BranchType, str]):
        self.all_commits_with_missing_jira_id: Dict[BranchType, List[CommitData]] = {}
        self.config = conf
        self.repo = repo
        self.branch_data: Dict[BranchType, BranchData] = {}
        for br_type in BranchType:
            branch_name = branch_dict[br_type]
            self.branch_data[br_type] = BranchData(br_type, branch_name)

        self.commit_matcher: CommitMatcherBase = self.config.matching_algorithm.matcher_class(self.branch_data)
        self.output_manager = self.config.matching_algorithm.output_manager_class(self.config, branch_dict)

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
                # Simply skip commits that doesn't have jira_id set to a valid value, as they are None.
                # Commits without jira_id handled separately with BranchData.set_commit_objs.
                if not commit.jira_id:
                    continue

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
            self.branch_data[br_type].set_merge_base(self.merge_base)

    def compare(self) -> MatchingResultBase:
        # Let the game begin :) --> Start to compare / A.K.A. match commits
        matching_result: MatchingResultBase = self.commit_matcher.match_commits(
            self.config, self.output_manager, self.merge_base, self
        )
        self.output_manager.write_commit_match_result_files(self.branch_data, matching_result)
        self.output_manager.print_and_save_summary(matching_result.rendered_summary)
        return matching_result

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


# TODO Add generic documentation
class BranchComparator(CommandAbs):
    def __init__(self, args, downstream_repo, upstream_repo, output_dir: str):
        branch_names: Dict[BranchType, str] = {
            BranchType.FEATURE: args.feature_branch,
            BranchType.MASTER: args.master_branch,
        }
        self.config = BranchComparatorConfig(output_dir, args, branch_names)
        if self.config.repo_type == RepoType.DOWNSTREAM:
            self.repo = downstream_repo
        elif self.config.repo_type == RepoType.UPSTREAM:
            self.repo = upstream_repo
        self.branches: Branches = Branches(self.config, self.repo, branch_names)
        self.matching_result = None

    @staticmethod
    def create_parser(subparsers):
        parser = subparsers.add_parser(
            CommandType.BRANCH_COMPARATOR.name,
            help="Branch comparator."
            "Usage: <algorithm> <feature branch> <master branch>"
            "Example: simple CDH-7.1-maint cdpd-master"
            "Example: grouped CDH-7.1-maint cdpd-master",
        )

        parser.add_argument(
            "algorithm",
            type=CommitMatchingAlgorithm.argparse,
            choices=list(CommitMatchingAlgorithm),
            help="Matcher algorithm",
        )
        parser.add_argument("feature_branch", type=str, help="Feature branch")
        parser.add_argument("master_branch", type=str, help="Master branch")
        parser.add_argument(
            "--commit_author_exceptions",
            type=str,
            nargs="+",
            help="Commits with these authors will be ignored while comparing branches",
        )
        parser.add_argument(
            "--console-mode",
            action="store_true",
            help="Console mode: Instead of writing output files, print everything to the console",
        )
        parser.add_argument(
            "--run-legacy-script",
            action="store_true",
            default=False,
            help="Console mode: Instead of writing output files, print everything to the console",
        )

        repo_types = [rt.value for rt in RepoType]
        parser.add_argument(
            "--repo-type",
            default=RepoType.DOWNSTREAM.value,
            choices=repo_types,
            help=f"Repo type, can be one of: {repo_types}",
        )
        parser.set_defaults(func=BranchComparator.execute)

    @staticmethod
    def execute(args, parser=None):
        output_dir = ProjectUtils.get_output_child_dir(CommandType.BRANCH_COMPARATOR.output_dir_name)
        branch_comparator = BranchComparator(
            args, YarnDevToolsConfig.DOWNSTREAM_REPO, YarnDevToolsConfig.UPSTREAM_REPO, output_dir
        )
        FileUtils.create_symlink_path_dir(
            CommandType.BRANCH_COMPARATOR.session_link_name,
            branch_comparator.config.output_dir,
            YarnDevToolsConfig.PROJECT_OUT_ROOT,
        )
        branch_comparator.run()

    def run(self):
        self.config.full_cmd = OsUtils.determine_full_command()
        LOG.info(f"Starting Branch comparator... \n{str(self.config)}")

        # TODO The following command fails:
        # Details: Misaligned table, missing column
        #  /Users/snemeth/Library/Caches/pypoetry/virtualenvs/yarn-dev-tools-iXlDyBPC-py3.8/bin/python /Users/snemeth/development/my-repos/yarn-dev-tools/yarndevtools/yarn_dev_tools.py --debug BRANCH_COMPARATOR simple origin/CDH-7.2.15.x origin/CDH-7.1.8.x --commit_author_exceptions rel-eng@cloudera.com --repo-type downstream
        # TODO fetch here before doing validation
        self.validate_branches()
        # TODO Make fetching optional, argparse argument
        # self.repo.fetch(all=True)
        self.matching_result = self.compare()
        if self.config.run_legacy_script:
            LegacyScriptRunner.start(self.config, self.branches, self.repo.repo_path, self.matching_result)

    def validate_branches(self):
        both_exist = self.branches.validate(BranchType.FEATURE)
        both_exist &= self.branches.validate(BranchType.MASTER)
        if not both_exist:
            raise ValueError("Both feature and master branch should be an existing branch. Exiting...")

    def compare(self) -> MatchingResultBase:
        self.branches.execute_git_log()
        self.branches.pre_compare()
        return self.branches.compare()
