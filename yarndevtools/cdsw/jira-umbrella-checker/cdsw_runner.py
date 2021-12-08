#!/usr/bin/env python3

import logging
from typing import List

from googleapiwrapper.google_drive import DriveApiFile

from yarndevtools.argparser import CommandType
from yarndevtools.cdsw.common_python.cdsw_common import (
    CdswRunnerBase,
    CdswSetup,
    CdswSetupResult,
)
from yarndevtools.cdsw.common_python.constants import CdswEnvVar, JIRA_UMBRELLA_CHECKER_DIR_NAME
from yarndevtools.constants import SUMMARY_FILE_TXT
from pythoncommons.jira_utils import JiraUtils

DEFAULT_BRANCHES = "origin/CDH-7.1-maint origin/cdpd-master origin/CDH-7.1.6.x"

LOG = logging.getLogger(__name__)
CMD_LOG = logging.getLogger(__name__)


class CdswRunner(CdswRunnerBase):
    def start(self, setup_result: CdswSetupResult, cdsw_runner_script_path: str):
        self.start_common(setup_result, cdsw_runner_script_path)
        self.run_clone_downstream_repos_script(setup_result.basedir)
        self.run_clone_upstream_repos_script(setup_result.basedir)

        # umbrella_ids = ["YARN-10496", "YARN-6223", "YARN-8820"]
        umbrella_ids = ["YARN-10888", "YARN-10889"]
        self.run_upstream_umbrella_checker_and_send_mail(umbrella_ids)

    def run_upstream_umbrella_checker_and_send_mail(self, umbrella_jira_ids: List[str]):
        jira_ids_and_titles = self._fetch_umbrella_titles(umbrella_jira_ids)
        for umbrella_jira_id, title in jira_ids_and_titles.items():
            cmd_type = CommandType.FETCH_JIRA_UMBRELLA_DATA
            self._run_upstream_umbrella_checker(umbrella_jira_id, branches=DEFAULT_BRANCHES)
            self.run_zipper(cmd_type, debug=True)

            sender = "YARN upstream umbrella checker"
            subject = f"YARN Upstream umbrella checker report: [UMBRELLA: {umbrella_jira_id} ({title}), start date: {self.start_date_str}]"
            command_data_filename: str = f"command_data_{self.start_date_str}.zip"
            kwargs = {
                "attachment_filename": command_data_filename,
                "email_body_file": SUMMARY_FILE_TXT,
                "send_attachment": True,
            }
            if self.is_drive_integration_enabled:
                drive_api_file: DriveApiFile = self.upload_command_data_to_drive(cmd_type, command_data_filename)
                link_text = f'<a href="{drive_api_file.link}">Command data file: {command_data_filename}</a>'
                kwargs["prepend_text_to_email_body"] = link_text
                kwargs["send_attachment"] = False
            self.send_latest_command_data_in_email(
                sender=sender,
                subject=subject,
                **kwargs,
            )

    @staticmethod
    def _fetch_umbrella_titles(jira_ids: List[str]):
        return {j_id: CdswRunner._fetch_umbrella_title(j_id) for j_id in jira_ids}

    @staticmethod
    def _fetch_umbrella_title(jira_id: str):
        jira_html_file = f"/tmp/jira_{jira_id}.html"
        LOG.info("Fetching HTML of jira: %s", jira_id)
        jira_html = JiraUtils.download_jira_html("https://issues.apache.org/jira/browse/", jira_id, jira_html_file)
        return JiraUtils.parse_jira_title(jira_html)

    def _run_upstream_umbrella_checker(self, umbrella_jira, branches, force=True, ignore_changes=True):
        if not umbrella_jira:
            raise ValueError("Umbrella jira should be specified")
        if not branches:
            raise ValueError("Branches should be specified")

        exec_mode = "--force" if force else ""
        ignore_changes = "--ignore-changes" if ignore_changes else ""
        self.execute_yarndevtools_script(
            f"--debug {CommandType.FETCH_JIRA_UMBRELLA_DATA.name} "
            f"{umbrella_jira} "
            f"{exec_mode} "
            f"--branches {branches} "
            f"{ignore_changes}"
        )


if __name__ == "__main__":
    mandatory_env_vars = [CdswEnvVar.MAIL_ACC_USER.value, CdswEnvVar.MAIL_ACC_PASSWORD.value]
    setup_result: CdswSetupResult = CdswSetup.initial_setup(mandatory_env_vars=mandatory_env_vars)
    runner = CdswRunner()
    runner.start(setup_result, CdswRunnerBase.get_filename(JIRA_UMBRELLA_CHECKER_DIR_NAME))
