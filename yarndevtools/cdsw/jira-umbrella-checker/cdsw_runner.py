import logging
from typing import Dict

from yarndevtools.argparser import CommandType
from yarndevtools.cdsw.common_python.cdsw_common import (
    CdswRunnerBase,
    CdswSetup,
    YARN_DEV_TOOLS_ROOT_DIR,
    MAIL_ADDR_YARN_ENG_BP,
)
from yarndevtools.cdsw.common_python.constants import CdswEnvVar
from yarndevtools.constants import SUMMARY_FILE_TXT

DEFAULT_BRANCHES = "origin/CDH-7.1-maint origin/cdpd-master origin/CDH-7.1.6.x"

LOG = logging.getLogger(__name__)
CMD_LOG = logging.getLogger(__name__)

JIRA_INFO: Dict[str, str] = {"YARN-10496": "AQC", "YARN-6223": "GPU phase 1", "YARN-8820": "GPU phase 2"}


class CdswRunner(CdswRunnerBase):
    def start(self, basedir):
        LOG.info("Starting CDSW runner...")
        self.run_clone_downstream_repos_script(basedir)
        self.run_clone_upstream_repos_script(basedir)
        self.run_upstream_umbrella_checker_and_send_mail(umbrella_jira="YARN-10496")
        self.run_upstream_umbrella_checker_and_send_mail(umbrella_jira="YARN-6223")
        self.run_upstream_umbrella_checker_and_send_mail(umbrella_jira="YARN-8820")

    def run_upstream_umbrella_checker_and_send_mail(self, umbrella_jira: str):
        date_str = self.current_date_formatted()

        self._run_upstream_umbrella_checker(umbrella_jira, branches=DEFAULT_BRANCHES)

        self.run_zipper(CommandType.FETCH_JIRA_UMBRELLA_DATA, debug=True)

        additional_info: str = JIRA_INFO[umbrella_jira]
        sender = "YARN upstream umbrella checker"
        subject = f"YARN Upstream umbrella checker report: [UMBRELLA: {umbrella_jira} ({additional_info}), start date: {date_str}]"
        attachment_fname: str = f"command_data_{date_str}.zip"
        self.send_latest_command_data_in_email(
            sender=sender,
            subject=subject,
            attachment_filename=attachment_fname,
            recipients=MAIL_ADDR_YARN_ENG_BP,
            email_body_file=SUMMARY_FILE_TXT,
        )

    def _run_upstream_umbrella_checker(self, umbrella_jira, branches, force=True, ignore_changes=True):
        if not umbrella_jira:
            raise ValueError("Umbrella jira should be specified")
        if not branches:
            raise ValueError("Branches should be specified")

        exec_mode = "--force" if force else ""
        ignore_changes = "--ignore-changes" if ignore_changes else ""
        self.execute_yarndevtools_script(
            f"--debug {CommandType.FETCH_JIRA_UMBRELLA_DATA.val} "
            f"{umbrella_jira} "
            f"{exec_mode} "
            f"--branches {branches} "
            f"{ignore_changes}"
        )


if __name__ == "__main__":
    basedir = CdswSetup.initial_setup(mandatory_env_vars=[CdswEnvVar.MAIL_ACC_USER, CdswEnvVar.MAIL_ACC_PASSWORD])
    LOG.info(f"YARN Dev tools mirror root dir: {YARN_DEV_TOOLS_ROOT_DIR}")
    runner = CdswRunner()
    runner.start(basedir)
