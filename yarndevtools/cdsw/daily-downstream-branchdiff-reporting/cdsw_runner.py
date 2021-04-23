from yarndevtools.argparser import CommandType
from yarndevtools.cdsw.common_python.cdsw_common import (
    CdswRunnerBase,
    CdswSetup,
    MAIL_ADDR_YARN_ENG_BP,
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
        self.run_clone_repos_script(basedir)

        # TODO investigate why legacy script fails!
        self.run_comparator_and_send_mail(algorithm="simple", run_legacy_script=False)
        self.run_comparator_and_send_mail(algorithm="grouped", run_legacy_script=False)

    def run_comparator_and_send_mail(self, algorithm="simple", run_legacy_script=True):
        feature_branch = "origin/CDH-7.1-maint"
        master_branch = "origin/cdpd-master"
        authors_to_filter = "rel-eng@cloudera.com"
        date_str = self.current_date_formatted()
        attachment_fnname: str = f"command_data_{algorithm}_{date_str}.zip"
        self._run_comparator(
            master_branch,
            feature_branch,
            authors_to_filter,
            debug=True,
            algorithm=algorithm,
            run_legacy_script=run_legacy_script,
        )
        self._run_zipper(CommandType.BRANCH_COMPARATOR, debug=True)

        subject_suffix = f" [{algorithm} algorithm, start date: {date_str}]"
        self._send_latest_command_data_in_email(subject_suffix=subject_suffix, attachment_filename=attachment_fnname)

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

    def _run_zipper(self, command_type: CommandType, debug=False):
        debug_mode = "--debug" if debug else ""
        self.execute_yarndevtools_script(
            f"{debug_mode} "
            f"{CommandType.ZIP_LATEST_COMMAND_DATA.val} {command_type.val} "
            f"--dest_dir /tmp "
            f"--dest_filename command_data.zip "
        )

    def _send_latest_command_data_in_email(
        self, recipients=MAIL_ADDR_YARN_ENG_BP, subject_suffix="", attachment_filename=None
    ):
        sender = "YARN branch diff reporter"
        subject = f"YARN Daily branch diff report{subject_suffix}"
        attachment_filename_param = f"--attachment-filename {attachment_filename}" if attachment_filename else ""
        self.execute_yarndevtools_script(
            f"--debug send_latest_command_data "
            f"{self.common_mail_config.as_arguments()}"
            f'--subject "{subject}" '
            f'--sender "{sender}" '
            f'--recipients "{recipients}" '
            f"{attachment_filename_param}"
        )


if __name__ == "__main__":
    basedir = CdswSetup.initial_setup(mandatory_env_vars=[EnvVar.MAIL_ACC_USER, EnvVar.MAIL_ACC_PASSWORD])
    LOG.info(f"YARN Dev tools mirror root dir: {YARN_DEV_TOOLS_ROOT_DIR}")
    runner = CdswRunner()
    runner.start(basedir)
