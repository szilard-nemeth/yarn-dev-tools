import logging
import os
import time
from argparse import ArgumentParser
from enum import Enum
from typing import List

from googleapiwrapper.google_drive import DriveApiFile
from pythoncommons.file_utils import FileUtils
from pythoncommons.os_utils import OsUtils

from yarndevtools.cdsw.common.cdsw_common import CdswRunnerBase, CdswSetupResult, CdswSetup, CommonDirs
from yarndevtools.cdsw.common.cdsw_config import CdswJobConfigReader, CdswJobConfig, CdswRun
from yarndevtools.cdsw.common.constants import CdswEnvVar, BranchComparatorEnvVar
from yarndevtools.common.shared_command_utils import CommandType, RepoType

LOG = logging.getLogger(__name__)


class ExecutionMode(Enum):
    AUTO_DISCOVERY = ("DISCOVER_CONFIG_FILE", "auto_discovery")
    SPECIFIED_CONFIG_FILE = ("SPECIFIED_CONFIG_FILE", "specified_file_config")

    def __init__(self, value, cli_name):
        self.val = value
        self.cli_name = cli_name


class ArgParser:
    @staticmethod
    def parse_args():
        parser = ArgumentParser()
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            dest="verbose",
            default=None,
            required=False,
            help="More verbose log (including gitpython verbose logs)",
        )
        parser.add_argument(
            "-d",
            "--debug",
            action="store_true",
            dest="debug",
            default=None,
            required=False,
            help="Turn on console debug level logs",
        )
        parser.add_argument(
            "cmd_type",
            type=str,
            choices=[e.name for e in CommandType],
            help="Type of command.",
        )

        parser.add_argument(
            "--dry-run",
            dest="dry_run",
            action="store_true",
            default=False,
            help="Dry run",
        )
        parser.add_argument("--config-file", type=str, help="Full path to job config file (JSON format)")

        args = parser.parse_args()
        if args.verbose:
            print("Args: " + str(args))
        return args, parser


class NewCdswConfigReaderAdapter:
    def read_from_file(self, file: str):
        return CdswJobConfigReader.read_from_file(file)


class NewCdswRunnerConfig:
    def __init__(self, parser, args, config_reader: NewCdswConfigReaderAdapter = None):
        self._validate_args(parser, args)
        self.full_cmd: str = OsUtils.determine_full_command_filtered(filter_password=True)
        self.execution_mode = self.determine_execution_mode(args)
        if self.execution_mode == ExecutionMode.SPECIFIED_CONFIG_FILE:
            self.job_config_file = args.config_file
        elif self.execution_mode == ExecutionMode.AUTO_DISCOVERY:
            LOG.info("Trying to discover config file for command: %s", self.command_type)
            # TODO implement discovery
            pass
        self._parse_command_type(args)
        self.dry_run = args.dry_run
        self.config_reader = config_reader

    def _parse_command_type(self, args):
        enum_vals = {ct.name: ct for ct in CommandType}
        if args.cmd_type not in enum_vals:
            raise ValueError("Invalid command type specified! Possible values are: {}".format(enum_vals))
        self.command_type = CommandType[args.cmd_type]

    @staticmethod
    def _validate_args(parser, args):
        pass

    @staticmethod
    def determine_execution_mode(args):
        # If there's no --config-file specified, it means auto-discovery
        if not hasattr(args, "config_file"):
            LOG.info("Config file not specified! Activated mode: %s", ExecutionMode.AUTO_DISCOVERY)
            return ExecutionMode.AUTO_DISCOVERY
        return ExecutionMode.SPECIFIED_CONFIG_FILE

    def __str__(self):
        return f"Full command: {self.full_cmd}\n"


class NewCdswRunner(CdswRunnerBase):
    def __init__(self, config: NewCdswRunnerConfig):
        super().__init__(dry_run=config.dry_run)
        self.cdsw_runner_config = config
        self.dry_run = config.dry_run
        self.job_config: CdswJobConfig = config.config_reader.read_from_file(config.job_config_file)
        self.command_type = self._determine_command_type()

    # TODO Rename later
    def begin(self):
        setup_result: CdswSetupResult = CdswSetup.initial_setup(mandatory_env_vars=self.job_config.mandatory_env_vars)
        self._execute_preparation_steps(setup_result)
        self.start(setup_result, None)

    def _determine_command_type(self):
        if self.cdsw_runner_config.command_type != self.job_config.command_type:
            raise ValueError(
                "Specified command line command type is different than job's command type. CLI: {}, Job definition: {}".format(
                    self.cdsw_runner_config.command_type, self.job_config.command_type
                )
            )
        return self.job_config.command_type

    def start(self, setup_result: CdswSetupResult, cdsw_runner_script_path: str):
        self.start_common(setup_result, cdsw_runner_script_path)
        self._execute_runs()

    def _execute_runs(self):
        runs: List[CdswRun] = self.job_config.runs
        for run in runs:
            self._execute_yarn_dev_tools(run)
            self._execute_command_data_zipper()
            drive_link_html_text = self._upload_command_data_to_google_drive_if_required(run)
            self._send_email_if_required(run, drive_link_html_text)

    def _execute_yarn_dev_tools(self, run: CdswRun):
        args = run.yarn_dev_tools_arguments
        args_as_string = " ".join(args)
        self.execute_yarndevtools_script(args_as_string)

    def _execute_command_data_zipper(self):
        self.run_zipper(self.command_type, debug=True)

    def _upload_command_data_to_google_drive_if_required(self, run: CdswRun):
        if not self.is_drive_integration_enabled:
            LOG.info(
                "Google Drive integration is disabled with env var '%s'!",
                CdswEnvVar.ENABLE_GOOGLE_DRIVE_INTEGRATION.value,
            )
            return
        if not run.drive_api_upload_settings:
            LOG.info("Google Drive upload settings is not defined for run: %s", run.name)
            return
        if not run.drive_api_upload_settings.enabled:
            LOG.info("Google Drive upload is disabled for run: %s", run.name)
            return

        drive_filename = run.drive_api_upload_settings.file_name
        if not self.dry_run:
            drive_api_file: DriveApiFile = self.upload_command_data_to_drive(self.command_type, drive_filename)
            self.google_drive_uploads.append((self.command_type, drive_filename, drive_api_file))
            return f'<a href="{drive_api_file.link}">Command data file: {drive_filename}</a>'
        else:
            LOG.info(
                "[DRY-RUN] Would upload file for command type '%s' to Google Drive with name '%s'",
                self.command_type,
                drive_filename,
            )
            return f'<a href="dummy_link">Command data file: {drive_filename}</a>'

    def _send_email_if_required(self, run: CdswRun, drive_link_html_text: str or None):
        if not run.email_settings:
            LOG.info("Email settings is not defined for run: %s", run.name)
            return
        if not run.email_settings.enabled:
            LOG.info("Email sending is disabled for run: %s", run.name)
            return

        # TODO make 'email_body_file' accept only enum values of these + Commands should also use this enum
        # SUMMARY_FILE_TXT = "summary.txt"
        # SUMMARY_FILE_HTML = "summary.html"
        # REPORT_FILE_SHORT_TXT = "report-short.txt"
        # REPORT_FILE_DETAILED_TXT = "report-detailed.txt"
        # REPORT_FILE_SHORT_HTML = "report-short.html"
        # REPORT_FILE_DETAILED_HTML = "report-detailed.html"
        kwargs = {
            "attachment_filename": run.email_settings.attachment_file_name,
            "email_body_file": run.email_settings.email_body_file_from_command_data,
            "send_attachment": True,
        }
        if drive_link_html_text:
            kwargs["prepend_text_to_email_body"] = drive_link_html_text

        LOG.debug("kwargs for email: %s", kwargs)
        self.send_latest_command_data_in_email(
            sender=run.email_settings.sender,
            subject=run.email_settings.subject,
            **kwargs,
        )

    def _execute_preparation_steps(self, setup_result):
        if self.command_type == CommandType.JIRA_UMBRELLA_DATA_FETCHER:
            self.run_clone_downstream_repos_script(setup_result.basedir)
            self.run_clone_upstream_repos_script(setup_result.basedir)
        elif self.command_type == CommandType.BRANCH_COMPARATOR:
            repo_type_env = OsUtils.get_env_value(
                BranchComparatorEnvVar.BRANCH_COMP_REPO_TYPE.value, RepoType.DOWNSTREAM.value
            )
            repo_type: RepoType = RepoType[repo_type_env.upper()]

            if repo_type == RepoType.DOWNSTREAM:
                self.run_clone_downstream_repos_script(setup_result.basedir)
            elif repo_type == RepoType.UPSTREAM:
                # If we are in upstream mode, make sure downstream dir exist
                # Currently, yarndevtools requires both repos to be present when initializing.
                # BranchComparator is happy with one single repository, upstream or downstream, exclusively.
                # Git init the other repository so everything will be alright
                FileUtils.create_new_dir(CommonDirs.HADOOP_CLOUDERA_BASEDIR)
                FileUtils.change_cwd(CommonDirs.HADOOP_CLOUDERA_BASEDIR)
                os.system("git init")
                self.run_clone_upstream_repos_script(setup_result.basedir)


if __name__ == "__main__":
    start_time = time.time()

    args, parser = ArgParser.parse_args()
    end_time = time.time()
    config = NewCdswRunnerConfig(parser, args)
    cdsw_runner = NewCdswRunner(config)
    cdsw_runner.begin()

    LOG.info("Execution of script took %d seconds", end_time - start_time)
