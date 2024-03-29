import logging
import unittest
from unittest.mock import Mock

from git import Commit
from pythoncommons.git_wrapper import GitWrapper
from pythoncommons.project_utils import ProjectUtils

from tests.test_utilities import Object, TestUtilities, SANDBOX_REPO_DOWNSTREAM_HOTFIX
from yarndevtools.commands.branchcomparator.branch_comparator import Branches, CommitMatchingAlgorithm, BranchComparator
from yarndevtools.commands.branchcomparator.common import BranchType
from yarndevtools.commands.branchcomparator.group_matching import GroupedMatchingResult
from yarndevtools.common.shared_command_utils import RepoType, CommandType
from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME, TRUNK

BRANCHES_CLASS_NAME = Branches.__name__
REPO_PATCH = "yarndevtools.commands.branchcomparator.branch_comparator.{}.send_mail".format(BRANCHES_CLASS_NAME)
FEATURE_BRANCH = "origin/CDH-7.1-maint"
MASTER_BRANCH = "origin/cdpd-master"
DEFAULT_COMMIT_AUTHOR_EXCEPTIONS = "rel-eng@cloudera.com"
YARN_TEST_BRANCH = TRUNK

LOG = logging.getLogger(__name__)


class TestBranchComparator(unittest.TestCase):
    upstream_repo_wrapper = None
    downstream_repo_wrapper = None
    downstream_utils = None
    upstream_utils = None
    log_dir = None
    sandbox_hadoop_repo_path = None

    @classmethod
    def setUpClass(cls):
        # Invoke this to set up main output directory and avoid test failures while initing config
        cls.project_out_root = ProjectUtils.get_test_output_basedir(YARNDEVTOOLS_MODULE_NAME)
        ProjectUtils.get_test_output_child_dir(CommandType.BRANCH_COMPARATOR.output_dir_name)

        # Upstream
        cls.upstream_utils = TestUtilities(cls, YARN_TEST_BRANCH)
        cls.upstream_utils.setUpClass(CommandType.BRANCH_COMPARATOR, init_logging=True, console_debug=True)
        cls.upstream_utils.pull_to_trunk(ff_only=True)
        cls.upstream_repo_wrapper = cls.upstream_utils.repo_wrapper

        # Downstream
        cls.downstream_utils = TestUtilities(cls, YARN_TEST_BRANCH)
        cls.downstream_utils.setUpClass(
            CommandType.BACKPORT, repo_postfix=SANDBOX_REPO_DOWNSTREAM_HOTFIX, init_logging=False
        )
        cls.downstream_utils.pull_to_trunk(ff_only=True)
        cls.downstream_repo_wrapper = cls.downstream_utils.repo_wrapper
        # Setup debug logging of git commands
        cls.downstream_repo_wrapper.enable_debug_logging(full=True)

    @classmethod
    def tearDownClass(cls) -> None:
        TestUtilities.tearDownClass(cls.__name__, command_type=CommandType.BRANCH_COMPARATOR)

    def setUp(self):
        self.upstream_utils.reset_and_checkout_existing_branch(YARN_TEST_BRANCH, pull=False)

    def tearDown(self) -> None:
        pass

    @staticmethod
    def generate_args(
        algorithm: CommitMatchingAlgorithm = CommitMatchingAlgorithm.GROUPED,
        repo_type: str = RepoType.DOWNSTREAM.value,
        feature_br: str = FEATURE_BRANCH,
        master_br: str = MASTER_BRANCH,
        commit_author_exceptions: str = DEFAULT_COMMIT_AUTHOR_EXCEPTIONS,
        debug: bool = False,
        run_legacy_script: bool = False,
    ):
        args = Object()
        args.debug = debug
        args.algorithm = algorithm
        args.repo_type = repo_type
        args.feature_branch = feature_br
        args.master_branch = master_br
        args.run_legacy_script = run_legacy_script
        if commit_author_exceptions:
            args.commit_author_exceptions = commit_author_exceptions

        return args

    @staticmethod
    def generate_args_e2e():
        # Full command:
        # /usr/local/lib/python3.8/site-packages/yarndevtools/yarn_dev_tools.py
        # --debug
        # BRANCH_COMPARATOR
        # simple
        # --repo-type upstream
        # origin/branch-3.3 origin/trunk
        # --commit_author_exceptions rel-eng@cloudera.com
        return TestBranchComparator.generate_args(
            debug=True,
            algorithm=CommitMatchingAlgorithm.SIMPLE,
            repo_type=RepoType.UPSTREAM.value,
            feature_br="origin/branch-3.3",
            master_br="origin/trunk",
            # TODO Maybe not required for upstream tests
            commit_author_exceptions="rel-eng@cloudera.com",
            run_legacy_script=False,
        )

    @property
    def output_dir(self):
        return ProjectUtils.get_test_output_child_dir(CommandType.BRANCH_COMPARATOR.output_dir_name)

    @staticmethod
    def _create_mock_merge_base(downstream_repo, merge_base_hash):
        mock_merge_base = Mock(spec=Commit)
        mock_merge_base.hexsha = merge_base_hash
        downstream_repo.merge_base.return_value = [mock_merge_base]

    def test_grouping(self):
        merge_base_hash = "99999999999"

        def git_log_return(revision, **kwargs):
            if revision == merge_base_hash:
                return [merge_base_commit_line]
            return log_lines

        # Fields: <hash> <commit message> <date> <author> <committer>
        merge_base_commit_line = (
            "99999999999 CDPD-999999. merge base 2001-11-29T04:35:52-08:00 stevel@cloudera.com stevel@cloudera.com"
        )
        log_lines = [
            'bee136f9b26e06b128ecaf90a751471f6b3b671e CDPD-31036. Revert "COMPX-6716: HDFS-16129. Fixing the signature secret file misusage in HttpFS. Contributed by Tamas Domok" 2021-11-19T08:12:11+01:00 tdomok@cloudera.com ',
            '3da9bd533a3299be854991432bfc43d6ff5277b8 CDPD-31036. Revert "COMPX-7434: HADOOP-16314. Make sure all web end points are covered by the same authentication filter. Contributed by Prabhu Joseph" 2021-11-19T08:12:09+01:00 hkoneru@cloudera.com tdomok@cloudera.com',
            "492e66a5f697b95f611420765f629a24c093d8e8 COMPX-7434: HADOOP-16314. Make sure all web end points are covered by the same authentication filter. Contributed by Prabhu Joseph 2021-10-05T07:25:16-07:00 eyang@apache.org tdomok@cloudera.com",
            "f340de85686e50e8225c8aaacf5b958d85b35b35 COMPX-6716: HDFS-16129. Fixing the signature secret file misusage in HttpFS. Contributed by Tamas Domok 2021-09-28T04:17:22-07:00 tdomok@cloudera.com tdomok@cloudera.com",
            merge_base_commit_line,
        ]
        downstream_repo: GitWrapper = Mock(spec=GitWrapper)
        downstream_repo.is_branch_exist.return_value = True
        downstream_repo.log.side_effect = git_log_return

        self._create_mock_merge_base(downstream_repo, merge_base_hash)
        upstream_repo = Mock(spec=GitWrapper)
        comparator = BranchComparator(self.generate_args(), downstream_repo, upstream_repo, self.output_dir)
        comparator.run()
        self.assertIsNotNone(comparator.matching_result)
        self.assertTrue(isinstance(comparator.matching_result, GroupedMatchingResult))

        # List of tuple of CommitGroups
        self.assertTrue(len(comparator.matching_result.matched_groups) == 2)

        self.assertTrue(len(comparator.matching_result.matched_groups[0]) == 2)
        self._assert_commit_group(comparator, index=0, hashes=["99999999999"])
        self._assert_commit_group(
            comparator,
            index=1,
            hashes=[
                "f340de85686e50e8225c8aaacf5b958d85b35b35",
                "492e66a5f697b95f611420765f629a24c093d8e8",
                "3da9bd533a3299be854991432bfc43d6ff5277b8",
                "bee136f9b26e06b128ecaf90a751471f6b3b671e",
            ],
        )

    def _assert_commit_group(self, comparator, index, hashes):
        master_group = comparator.matching_result.matched_groups[index][0]
        self.assertEqual(BranchType.MASTER, master_group.br_type)
        self.assertEqual(hashes, master_group.commit_hashes)

        feature_group = comparator.matching_result.matched_groups[index][1]
        self.assertEqual(BranchType.FEATURE, feature_group.br_type)
        self.assertEqual(hashes, master_group.commit_hashes)

    def test_e2e_upstream(self):
        comparator = BranchComparator(
            self.generate_args_e2e(), self.downstream_repo_wrapper, self.upstream_repo_wrapper, self.output_dir
        )
        comparator.run()
