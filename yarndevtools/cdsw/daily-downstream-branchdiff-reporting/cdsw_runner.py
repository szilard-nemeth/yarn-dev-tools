from yarndevtools.argparser import CommandType
from yarndevtools.cdsw.common_python.cdsw_common import (
    CdswRunnerBase,
    CdswSetup,
    YARN_DEV_TOOLS_ROOT_DIR,
)
from yarndevtools.cdsw.common_python.constants import EnvVar
import logging

LOG = logging.getLogger(__name__)
CMD_LOG = logging.getLogger(__name__)
ENV_OVERRIDE_SCRIPT_BASEDIR = "OVERRIDE_SCRIPT_BASEDIR"


class CdswRunner(CdswRunnerBase):
    def start(self, basedir):
        LOG.info("Starting CDSW runner...")
        self.run_clone_downstream_repos_script(basedir)

        # TODO investigate why legacy script fails!
        self.run_comparator_and_send_mail(algorithm="simple", run_legacy_script=False)
        self.run_comparator_and_send_mail(algorithm="grouped", run_legacy_script=False)

    def run_comparator_and_send_mail(self, algorithm="simple", run_legacy_script=True):
        date_str = self.current_date_formatted()
        feature_branch = "origin/CDH-7.1-maint"
        master_branch = "origin/cdpd-master"
        authors_to_filter = "rel-eng@cloudera.com"
        self._run_comparator(
            master_branch,
            feature_branch,
            authors_to_filter,
            debug=True,
            algorithm=algorithm,
            run_legacy_script=run_legacy_script,
        )

        self.run_zipper(CommandType.BRANCH_COMPARATOR, debug=True)

        sender = "YARN branch diff reporter"
        subject = f"YARN Daily branch diff report [{algorithm} algorithm, start date: {date_str}]"
        attachment_fnname: str = f"command_data_{algorithm}_{date_str}.zip"
        self.send_latest_command_data_in_email(sender=sender, subject=subject, attachment_filename=attachment_fnname)

    def _run_comparator(
        self, master_branch, feature_branch, authors_to_filter, debug=False, algorithm="simple", run_legacy_script=True
    ):
        debug_mode = "--debug" if debug else ""
        run_legacy_script_str = "--run-legacy-script" if run_legacy_script else ""
        self.execute_yarndevtools_script(
            f"{debug_mode} "
            f"{CommandType.BRANCH_COMPARATOR.val} {algorithm} {feature_branch} {master_branch} "
            f"--commit_author_exceptions {authors_to_filter} "
            f"{run_legacy_script_str}"
        )


if __name__ == "__main__":
    basedir = CdswSetup.initial_setup(mandatory_env_vars=[EnvVar.MAIL_ACC_USER, EnvVar.MAIL_ACC_PASSWORD])
    LOG.info(f"YARN Dev tools mirror root dir: {YARN_DEV_TOOLS_ROOT_DIR}")
    runner = CdswRunner()
    runner.start(basedir)
