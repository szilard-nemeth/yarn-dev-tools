import logging
import unittest
from unittest.mock import Mock

from git import Commit
from pythoncommons.git_wrapper import GitWrapper
from pythoncommons.project_utils import ProjectUtils

from tests.test_utilities import Object, TestUtilities
from yarndevtools.commands.branchcomparator.branch_comparator import Branches, CommitMatchingAlgorithm, BranchComparator
from yarndevtools.common.shared_command_utils import RepoType
from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME, BRANCH_COMPARATOR

BRANCHES_CLASS_NAME = Branches.__name__
REPO_PATCH = "yarndevtools.commands.branchcomparator.branch_comparator.{}.send_mail".format(BRANCHES_CLASS_NAME)
FEATURE_BRANCH = "origin/CDH-7.1-maint"
MASTER_BRANCH = "origin/cdpd-master"
DEFAULT_COMMIT_AUTHOR_EXCEPTIONS = "rel-eng@cloudera.com"

LOG = logging.getLogger(__name__)


class TestBranchComparator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Invoke this to setup main output directory and avoid test failures while initing config
        cls.project_out_root = ProjectUtils.get_test_output_basedir(YARNDEVTOOLS_MODULE_NAME)
        ProjectUtils.get_test_output_child_dir(BRANCH_COMPARATOR)

    @classmethod
    def tearDownClass(cls) -> None:
        TestUtilities.tearDownClass(cls.__name__)

    def setUp(self):
        pass

    def tearDown(self) -> None:
        pass

    @staticmethod
    def generate_args(
        algorithm: CommitMatchingAlgorithm = CommitMatchingAlgorithm.GROUPED,
        repo_type: str = RepoType.DOWNSTREAM.value,
        feature_br: str = FEATURE_BRANCH,
        master_br: str = MASTER_BRANCH,
        commit_author_exceptions: str = DEFAULT_COMMIT_AUTHOR_EXCEPTIONS,
    ):
        args = Object()
        args.algorithm = algorithm
        args.repo_type = repo_type
        args.feature_branch = feature_br
        args.master_branch = master_br
        args.run_legacy_script = False
        if commit_author_exceptions:
            args.commit_author_exceptions = commit_author_exceptions
        return args

    @property
    def output_dir(self):
        return ProjectUtils.get_test_output_child_dir(BRANCH_COMPARATOR)

    def test_grouping(self):
        def git_log_return(revision, **kwargs):
            if revision == "1214312515":
                return [merge_base_commit_line]
            return log_lines

        # Fields: <hash> <commit message> <date> <author> <committer>
        merge_base_commit_line = (
            "99999999999 CDPD-999999. merge base 2001-11-29T04:35:52-08:00 stevel@cloudera.com stevel@cloudera.com"
        )
        log_lines = [
            'bee136f9b26e06b128ecaf90a751471f6b3b671e CDPD-31036. Revert "COMPX-6716: HDFS-16129. Fixing the signature secret file misusage in HttpFS. Contributed by Tamas Domok" 2021-11-19T08:12:09+01:00 tdomok@cloudera.com ',
            '3da9bd533a3299be854991432bfc43d6ff5277b8 CDPD-31036. Revert "COMPX-7434: HADOOP-16314. Make sure all web end points are covered by the same authentication filter. Contributed by Prabhu Joseph" 2021-11-19T08:12:09+01:00 hkoneru@cloudera.com tdomok@cloudera.com',
            "492e66a5f697b95f611420765f629a24c093d8e8 COMPX-7434: HADOOP-16314. Make sure all web end points are covered by the same authentication filter. Contributed by Prabhu Joseph 2021-10-05T07:25:16-07:00 eyang@apache.org tdomok@cloudera.com",
            "f340de85686e50e8225c8aaacf5b958d85b35b35 COMPX-6716: HDFS-16129. Fixing the signature secret file misusage in HttpFS. Contributed by Tamas Domok 2021-09-28T04:17:22-07:00 tdomok@cloudera.com tdomok@cloudera.com",
            merge_base_commit_line,
        ]
        downstream_repo: GitWrapper = Mock(spec=GitWrapper)
        downstream_repo.is_branch_exist.return_value = True
        downstream_repo.log.side_effect = git_log_return

        mock_merge_base = Mock(spec=Commit)
        mock_merge_base.hexsha = "1214312515"
        downstream_repo.merge_base.return_value = [mock_merge_base]
        upstream_repo = Mock(spec=GitWrapper)
        comparator = BranchComparator(self.generate_args(), downstream_repo, upstream_repo, self.output_dir)
        comparator.run()
