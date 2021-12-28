import logging
import unittest

from pythoncommons.file_utils import FileUtils
from pythoncommons.project_utils import ProjectUtils

from yarndevtools.common.shared_command_utils import CommandType
from yarndevtools.commands.upstreamumbrellafetcher.upstream_jira_umbrella_fetcher import UpstreamJiraUmbrellaFetcher
from yarndevtools.constants import TRUNK, JIRA_UMBRELLA_DATA, ORIGIN_TRUNK, ORIGIN_BRANCH_3_3, ORIGIN_BRANCH_3_2
from tests.test_utilities import TestUtilities, Object

FILE_JIRA_HTML = "jira.html"
FILE_SUMMARY_TXT = "summary.txt"
FILE_SUMMARY_HTML = "summary.html"
FILE_JIRA_LIST = "jira-list.txt"
FILE_INTERMEDIATE_RESULTS = "intermediate-results.txt"
FILE_COMMIT_HASHES_TEMPLATE = "commit-hashes_$BRANCH.txt"
FILE_CHANGED_FILES = "changed-files.txt"
ALL_OUTPUT_FILES = [
    FILE_JIRA_HTML,
    FILE_JIRA_LIST,
    FILE_SUMMARY_TXT,
    FILE_SUMMARY_HTML,
    FILE_INTERMEDIATE_RESULTS,
    FILE_CHANGED_FILES,
]
IGNORE_CHANGES_MODE_OUTPUT_FILES = [
    FILE_JIRA_HTML,
    FILE_JIRA_LIST,
    FILE_SUMMARY_TXT,
    FILE_SUMMARY_HTML,
    FILE_INTERMEDIATE_RESULTS,
]
IGNORE_CHANGES_MODE_MODIFIED_FILE_LIST = [FILE_SUMMARY_TXT, FILE_SUMMARY_HTML]


UPSTREAM_JIRA_ID = "YARN-5734"
AQC_PHASE1_UPSTREAM_JIRA_ID = "YARN-10889"
UPSTREAM_JIRA_WITH_0_SUBJIRAS = "YARN-9629"
UPSTREAM_JIRA_NOT_EXISTING = "YARN-1111111"
UPSTREAM_JIRA_DOES_NOT_HAVE_COMMIT = "YARN-3525"
LOG = logging.getLogger(__name__)


class TestUpstreamJiraUmbrellaFetcher(unittest.TestCase):
    utils = None
    repo = None
    log_dir = None
    sandbox_hadoop_repo_path = None

    @classmethod
    def setUpClass(cls):
        cls.utils = TestUtilities(cls, None)
        cls.utils.setUpClass(CommandType.FETCH_JIRA_UMBRELLA_DATA)
        cls.utils.pull_to_trunk()
        cls.repo = cls.utils.repo
        cls.repo_wrapper = cls.utils.repo_wrapper
        cls.saved_patches_dir = cls.utils.saved_patches_dir
        cls.base_branch = TRUNK

        # Invoke this to set up main output directory and avoid test failures while initing config
        ProjectUtils.get_output_child_dir(JIRA_UMBRELLA_DATA)

        commit_hashes_file = TestUpstreamJiraUmbrellaFetcher.get_commit_hashes_filename_of_branch(ORIGIN_TRUNK)
        ALL_OUTPUT_FILES.append(commit_hashes_file)
        IGNORE_CHANGES_MODE_OUTPUT_FILES.append(commit_hashes_file)

    @classmethod
    def tearDownClass(cls) -> None:
        TestUtilities.tearDownClass(cls.__name__)

    def cleanup_and_checkout_branch(self, test_branch):
        self.utils.cleanup_and_checkout_test_branch(pull=False)
        self.assertEqual(test_branch, str(self.repo.head.ref))

    def setup_args(self, jira_id, force_mode=False, ignore_changes=False, branches=None):
        args = Object()
        args.jira_id = jira_id
        args.force_mode = force_mode
        args.ignore_changes = ignore_changes
        if branches:
            args.branches = branches
        return args

    def test_fetch_on_branch_other_than_trunk_fails(self):
        self.repo_wrapper.checkout_parent_of_branch(self.base_branch)

        # Can't use self.repo.head.ref as HEAD is a detached reference
        # self.repo.head.ref would raise: TypeError: HEAD is a detached symbolic reference as it points to
        self.assertNotEqual(self.repo_wrapper.get_hash_of_commit(self.base_branch), self.repo.head.commit.hexsha)
        umbrella_fetcher = UpstreamJiraUmbrellaFetcher(
            self.setup_args(UPSTREAM_JIRA_ID),
            self.repo_wrapper,
            self.repo_wrapper,
            self.utils.jira_umbrella_data_dir,
            self.base_branch,
        )
        self.assertRaises(ValueError, umbrella_fetcher.run)

    def test_fetch_with_upstream_jira_that_is_not_an_umbrella_works(self):
        self.utils.checkout_trunk()
        umbrella_fetcher = UpstreamJiraUmbrellaFetcher(
            self.setup_args(jira_id=UPSTREAM_JIRA_WITH_0_SUBJIRAS),
            self.repo_wrapper,
            self.repo_wrapper,
            self.utils.jira_umbrella_data_dir,
            self.base_branch,
        )
        try:
            umbrella_fetcher.run()
        except ValueError as e:
            self.assertTrue("Cannot find subjiras for jira with id" in str(e))

    def test_fetch_with_upstream_jira_not_existing(self):
        self.utils.checkout_trunk()
        umbrella_fetcher = UpstreamJiraUmbrellaFetcher(
            self.setup_args(jira_id=UPSTREAM_JIRA_NOT_EXISTING),
            self.repo_wrapper,
            self.repo_wrapper,
            self.utils.jira_umbrella_data_dir,
            self.base_branch,
        )
        self.assertRaises(ValueError, umbrella_fetcher.run)

    def test_fetch_with_upstream_jira_that_does_not_have_commit(self):
        self.utils.checkout_trunk()
        umbrella_fetcher = UpstreamJiraUmbrellaFetcher(
            self.setup_args(jira_id=UPSTREAM_JIRA_DOES_NOT_HAVE_COMMIT),
            self.repo_wrapper,
            self.repo_wrapper,
            self.utils.jira_umbrella_data_dir,
            self.base_branch,
        )
        self.assertRaises(ValueError, umbrella_fetcher.run)

    def test_fetch_with_upstream_umbrella_cached_mode(self):
        self.utils.checkout_trunk()
        umbrella_fetcher = UpstreamJiraUmbrellaFetcher(
            self.setup_args(force_mode=False, jira_id=UPSTREAM_JIRA_ID),
            self.repo_wrapper,
            self.repo_wrapper,
            self.utils.jira_umbrella_data_dir,
            self.base_branch,
        )
        # Run first, to surely have results pickled for this umbrella
        umbrella_fetcher.run()

        # Run again, with using cache
        umbrella_fetcher.run()

        output_dir = FileUtils.join_path(self.utils.jira_umbrella_data_dir, UPSTREAM_JIRA_ID)
        original_mod_dates = FileUtils.get_mod_dates_of_files(output_dir, *ALL_OUTPUT_FILES)

        self._verify_files_and_mod_dates(output_dir, files=ALL_OUTPUT_FILES)

        # Since we are using non-force mode (cached mode), we expect the files untouched
        new_mod_dates = FileUtils.get_mod_dates_of_files(output_dir, *ALL_OUTPUT_FILES)
        self.assertDictEqual(original_mod_dates, new_mod_dates)

    def _verify_files_and_mod_dates(self, output_dir, files):
        # Verify files and mod dates
        for out_file in files:
            self.utils.assert_file_not_empty(FileUtils.join_path(output_dir, out_file))

    def test_fetch_with_upstream_umbrella_force_mode(self):
        self.utils.checkout_trunk()
        output_dir = FileUtils.join_path(self.utils.jira_umbrella_data_dir, UPSTREAM_JIRA_ID)
        original_mod_dates = FileUtils.get_mod_dates_of_files(output_dir, *ALL_OUTPUT_FILES)
        umbrella_fetcher = UpstreamJiraUmbrellaFetcher(
            self.setup_args(force_mode=True, jira_id=UPSTREAM_JIRA_ID),
            self.repo_wrapper,
            self.repo_wrapper,
            self.utils.jira_umbrella_data_dir,
            self.base_branch,
        )
        umbrella_fetcher.run()

        self._verify_files_and_mod_dates(output_dir, files=ALL_OUTPUT_FILES)

        # Since we are using force-mode (non cached mode), we expect all files have a newer mod date
        new_mod_dates = FileUtils.get_mod_dates_of_files(output_dir, *ALL_OUTPUT_FILES)
        self._assert_mod_dates(original_mod_dates, new_mod_dates)

    def _assert_mod_dates(self, original_mod_dates, new_mod_dates):
        for file, mod_date in new_mod_dates.items():
            LOG.info("Checking mod date of file: %s", file)
            self.assertTrue(file in original_mod_dates, "Unknown old mod date for file: {}".format(file))
            self.assertTrue(
                original_mod_dates[file] is not None,
                "Unknown old mod date for file, mod date is None of: {}".format(file),
            )
            self.assertTrue(isinstance(mod_date, float), "New mod date is unknown for file: {}".format(file))
            self.assertTrue(mod_date > original_mod_dates[file], f"File has not been modified: {file}")

    def test_fetch_with_upstream_umbrella_ignore_changes_manual_mode(self):
        self.utils.checkout_trunk()
        output_dir = FileUtils.join_path(self.utils.jira_umbrella_data_dir, AQC_PHASE1_UPSTREAM_JIRA_ID)
        original_mod_dates = FileUtils.get_mod_dates_of_files(output_dir, *ALL_OUTPUT_FILES)
        branches = [ORIGIN_TRUNK, ORIGIN_BRANCH_3_3, ORIGIN_BRANCH_3_2]
        umbrella_fetcher = UpstreamJiraUmbrellaFetcher(
            self.setup_args(
                force_mode=True,
                ignore_changes=True,
                branches=branches,
                jira_id=AQC_PHASE1_UPSTREAM_JIRA_ID,
            ),
            self.repo_wrapper,
            self.repo_wrapper,
            self.utils.jira_umbrella_data_dir,
            self.base_branch,
        )
        umbrella_fetcher.run()

        files_to_check = IGNORE_CHANGES_MODE_OUTPUT_FILES + [self.get_commit_hashes_filename_of_branch(ORIGIN_TRUNK)]
        self._verify_files_and_mod_dates(output_dir, files=files_to_check)
        new_mod_dates = FileUtils.get_mod_dates_of_files(output_dir, *IGNORE_CHANGES_MODE_MODIFIED_FILE_LIST)
        self._assert_mod_dates(original_mod_dates, new_mod_dates)

    @classmethod
    def get_commit_hashes_filename_of_branch(cls, branch):
        branch = cls.convert_branch_name(branch)
        return FILE_COMMIT_HASHES_TEMPLATE.replace("$BRANCH", branch)

    @staticmethod
    def convert_branch_name(b):
        return b.replace("/", "_").replace(".", "_")


if __name__ == "__main__":
    unittest.main()
