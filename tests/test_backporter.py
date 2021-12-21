import logging
import unittest

from tests.test_utilities import TestUtilities, Object, SANDBOX_REPO_DOWNSTREAM_HOTFIX
from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.commands.backporter import Backporter
from pythoncommons.git_constants import ORIGIN
from yarndevtools.constants import TRUNK, BRANCH_3_1
from yarndevtools.yarn_dev_tools import DEFAULT_BASE_BRANCH

UPSTREAM_JIRA_ID = "YARN-123456: "
DOWNSTREAM_BRANCH = "cdh6x"
DOWNSTREAM_JIRA_ID = "CDH-1234"
UPSTREAM_REMOTE_NAME = "upstream"
FETCH = True

LOG = logging.getLogger(__name__)

# Commit should be in trunk, this is a prerequisite of the backporter
YARN_TEST_BRANCH = TRUNK
CHERRY_PICK_BASE_REF = TRUNK


class TestBackporter(unittest.TestCase):
    downstream_repo_wrapper = None
    downstream_utils = None
    upstream_utils = None
    upstream_repo = None
    log_dir = None
    sandbox_hadoop_repo_path = None

    @classmethod
    def setUpClass(cls):
        cls.upstream_utils = TestUtilities(cls, YARN_TEST_BRANCH)
        cls.upstream_utils.setUpClass(CommandType.BACKPORT_C6, init_logging=True, console_debug=True)
        cls.upstream_utils.pull_to_trunk(ff_only=True)
        cls.upstream_repo = cls.upstream_utils.repo
        cls.upstream_repo_wrapper = cls.upstream_utils.repo_wrapper

        cls.downstream_utils = TestUtilities(cls, YARN_TEST_BRANCH)
        cls.downstream_utils.setUpClass(
            CommandType.BACKPORT_C6, repo_postfix=SANDBOX_REPO_DOWNSTREAM_HOTFIX, init_logging=False
        )
        cls.downstream_utils.pull_to_trunk(ff_only=True)
        cls.downstream_repo = cls.downstream_utils.repo
        cls.downstream_repo_wrapper = cls.downstream_utils.repo_wrapper

        cls.full_ds_branch = f"{DOWNSTREAM_JIRA_ID}-{DOWNSTREAM_BRANCH}"
        cls.downstream_repo_wrapper.setup_committer_info("downstream_user", "downstream_email")
        # Setup debug logging of git commands
        cls.downstream_repo_wrapper.enable_debug_logging(full=True)

    @classmethod
    def tearDownClass(cls) -> None:
        TestUtilities.tearDownClass(cls.__name__)

    def setUp(self):
        self.upstream_utils.reset_and_checkout_existing_branch(YARN_TEST_BRANCH, pull=False)

        # THIS IS A MUST HAVE!
        # Set up remote of upstream in the downstream repo
        self.downstream_repo_wrapper.add_remote(UPSTREAM_REMOTE_NAME, self.upstream_repo.git_dir)
        self.downstream_repo_wrapper.remove_branch(self.full_ds_branch, checkout_before_remove=TRUNK)

    def setup_args(self):
        args = Object()
        args.upstream_jira_id = UPSTREAM_JIRA_ID
        args.upstream_branch = DEFAULT_BASE_BRANCH
        args.downstream_jira_id = DOWNSTREAM_JIRA_ID
        args.downstream_branch = DOWNSTREAM_BRANCH
        args.no_fetch = not FETCH
        return args

    def cleanup_and_checkout_branch(self, branch=None, checkout_from=None):
        if branch:
            self.upstream_utils.cleanup_and_checkout_test_branch(pull=False, branch=branch, checkout_from=checkout_from)
            self.assertEqual(branch, str(self.upstream_repo.head.ref))
        else:
            self.upstream_utils.cleanup_and_checkout_test_branch(pull=False, checkout_from=checkout_from)
            self.assertEqual(YARN_TEST_BRANCH, str(self.upstream_repo.head.ref))

    def test_with_uncommitted_should_raise_error(self):
        self.upstream_utils.add_some_file_changes(commit=False)
        args = self.setup_args()

        backporter = Backporter(args, self.upstream_repo_wrapper, self.downstream_repo_wrapper, CHERRY_PICK_BASE_REF)
        self.assertRaises(ValueError, backporter.run)

    def test_with_committed_with_wrong_message_should_raise_error(self):
        self.cleanup_and_checkout_branch()
        self.upstream_utils.add_some_file_changes(commit=True, commit_message_prefix="dummy")
        args = self.setup_args()

        backporter = Backporter(args, self.upstream_repo_wrapper, self.downstream_repo_wrapper, CHERRY_PICK_BASE_REF)
        self.assertRaises(ValueError, backporter.run)

    def test_with_committed_with_good_message_remote_to_upstream_does_not_exist(self):
        self.cleanup_and_checkout_branch()
        self.upstream_utils.add_some_file_changes(commit=True, commit_message_prefix=UPSTREAM_JIRA_ID)
        args = self.setup_args()

        # Intentionally remove remote
        self.downstream_repo_wrapper.remove_remote(UPSTREAM_REMOTE_NAME)

        backporter = Backporter(args, self.upstream_repo_wrapper, self.downstream_repo_wrapper, CHERRY_PICK_BASE_REF)
        self.assertRaises(ValueError, backporter.run)

    def test_with_committed_with_good_message(self):
        self.cleanup_and_checkout_branch()
        self.upstream_utils.add_some_file_changes(commit=True, commit_message_prefix=UPSTREAM_JIRA_ID)
        args = self.setup_args()

        backporter = Backporter(args, self.upstream_repo_wrapper, self.downstream_repo_wrapper, CHERRY_PICK_BASE_REF)
        backporter.run()

        expected_commit_msg = f"{DOWNSTREAM_JIRA_ID}: {UPSTREAM_JIRA_ID}test_commit"
        self.assertTrue(
            self.full_ds_branch in self.downstream_repo.heads,
            f"Created downstream branch does not exist: {self.full_ds_branch}",
        )
        self.downstream_utils.verify_commit_message_of_branch(
            self.full_ds_branch, expected_commit_msg, verify_cherry_picked_from=True
        )

    def test_backport_from_branch31(self):
        self.cleanup_and_checkout_branch(branch=BRANCH_3_1, checkout_from=ORIGIN + "/" + BRANCH_3_1)
        self.upstream_utils.add_some_file_changes(commit=True, commit_message_prefix=UPSTREAM_JIRA_ID)
        args = self.setup_args()
        args.upstream_branch = BRANCH_3_1

        backporter = Backporter(args, self.upstream_repo_wrapper, self.downstream_repo_wrapper, CHERRY_PICK_BASE_REF)
        backporter.run()

        expected_commit_msg = f"{DOWNSTREAM_JIRA_ID}: {UPSTREAM_JIRA_ID}test_commit"
        self.assertTrue(
            self.full_ds_branch in self.downstream_repo.heads,
            f"Created downstream branch does not exist: {self.full_ds_branch}",
        )
        self.downstream_utils.verify_commit_message_of_branch(
            self.full_ds_branch, expected_commit_msg, verify_cherry_picked_from=True
        )
