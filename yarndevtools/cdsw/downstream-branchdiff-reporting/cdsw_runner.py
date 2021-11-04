#!/usr/bin/env python3

import os

from pythoncommons.file_utils import FileUtils
from pythoncommons.os_utils import OsUtils

from yarndevtools.argparser import CommandType
from yarndevtools.cdsw.common_python.cdsw_common import CdswRunnerBase, CdswSetup, CommonDirs
from yarndevtools.cdsw.common_python.constants import CdswEnvVar, BranchComparatorEnvVar
import logging

from yarndevtools.common.shared_command_utils import RepoType

LOG = logging.getLogger(__name__)
CMD_LOG = logging.getLogger(__name__)
ENV_OVERRIDE_SCRIPT_BASEDIR = "OVERRIDE_SCRIPT_BASEDIR"


class CdswRunner(CdswRunnerBase):
    def start(self, basedir):
        self.start_common(basedir)
        repo_type_env = OsUtils.get_env_value(BranchComparatorEnvVar.REPO_TYPE.value, RepoType.DOWNSTREAM.value)
        repo_type: RepoType = RepoType[repo_type_env.upper()]

        if repo_type == RepoType.DOWNSTREAM:
            self.run_clone_downstream_repos_script(basedir)
        elif repo_type == RepoType.UPSTREAM:
            # If we are in upstream mode, make sure downstream dir exist
            # Currently, yarndevtools requires both repos to be present when initializing.
            # BranchComparator is happy with one single repository, upstream or downstream, exclusively.
            # Git init the other repository so everything will be alright
            FileUtils.create_new_dir(CommonDirs.HADOOP_CLOUDERA_BASEDIR)
            FileUtils.change_cwd(CommonDirs.HADOOP_CLOUDERA_BASEDIR)
            os.system("git init")
            self.run_clone_upstream_repos_script(basedir)

        # TODO investigate why legacy script fails!
        self.run_comparator_and_send_mail(repo_type, algorithm="simple", run_legacy_script=False)
        self.run_comparator_and_send_mail(repo_type, algorithm="grouped", run_legacy_script=False)

    def run_comparator_and_send_mail(self, repo_type: RepoType, algorithm="simple", run_legacy_script=True):
        date_str = self.current_date_formatted()
        feature_branch = OsUtils.get_env_value(BranchComparatorEnvVar.FEATURE_BRANCH.value, "origin/CDH-7.1-maint")
        master_branch = OsUtils.get_env_value(BranchComparatorEnvVar.MASTER_BRANCH.value, "origin/cdpd-master")
        authors_to_filter = "rel-eng@cloudera.com"
        self._run_comparator(
            repo_type,
            master_branch,
            feature_branch,
            authors_to_filter,
            debug=True,
            algorithm=algorithm,
            run_legacy_script=run_legacy_script,
        )

        self.run_zipper(CommandType.BRANCH_COMPARATOR, debug=True)

        sender = "YARN branch diff reporter"
        subject = f"YARN branch diff report [{algorithm} algorithm, start date: {date_str}]"
        attachment_fnname: str = f"command_data_{algorithm}_{date_str}.zip"
        self.send_latest_command_data_in_email(sender=sender, subject=subject, attachment_filename=attachment_fnname)

    def _run_comparator(
        self,
        repo_type: RepoType,
        master_branch: str,
        feature_branch: str,
        authors_to_filter,
        debug=False,
        algorithm="simple",
        run_legacy_script=True,
    ):
        debug_mode = "--debug" if debug else ""
        repo_type_val = f"--repo-type {repo_type.value}"
        run_legacy_script_str = "--run-legacy-script" if run_legacy_script else ""
        self.execute_yarndevtools_script(
            f"{debug_mode} "
            f"{CommandType.BRANCH_COMPARATOR.val} {algorithm} {repo_type_val} {feature_branch} {master_branch} "
            f"--commit_author_exceptions {authors_to_filter} "
            f"{run_legacy_script_str}"
        )


if __name__ == "__main__":
    basedir = CdswSetup.initial_setup(
        mandatory_env_vars=[CdswEnvVar.MAIL_ACC_USER.value, CdswEnvVar.MAIL_ACC_PASSWORD.value]
    )

    runner = CdswRunner()
    runner.start(basedir)
