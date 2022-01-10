import logging
import time
from argparse import ArgumentParser
from enum import Enum
from typing import List

from googleapiwrapper.google_drive import DriveApiFile
from pythoncommons.os_utils import OsUtils

from yarndevtools.cdsw.common_python.cdsw_common import CdswRunnerBase, CdswSetupResult, CdswSetup
from yarndevtools.cdsw.common_python.cdsw_config import CdswJobConfigReader, CdswJobConfig, CdswRun
from yarndevtools.common.shared_command_utils import CommandType

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


class NewCdswRunnerConfig:
    def __init__(self, parser, args):
        self._validate_args(parser, args)
        self.full_cmd: str = OsUtils.determine_full_command_filtered(filter_password=True)
        self.execution_mode = self.determine_execution_mode()
        if self.execution_mode == ExecutionMode.SPECIFIED_CONFIG_FILE:
            self.job_config_file = args.config_file
        elif self.execution_mode == ExecutionMode.AUTO_DISCOVERY:
            # TODO implement discovery
            pass
        self.command_type = CommandType[args.cmd_type]
        self.dry_run = args.dry_run

    @staticmethod
    def _validate_args(parser, args):
        pass

    def determine_execution_mode(self):
        # If there's no --config-file specified, it means auto-discovery
        if not hasattr(args, "config_file"):
            LOG.info("Config file not specified! Activated mode: %s", ExecutionMode.AUTO_DISCOVERY)
            return ExecutionMode.AUTO_DISCOVERY
        return ExecutionMode.SPECIFIED_CONFIG_FILE

    def __str__(self):
        return f"Full command: {self.full_cmd}\n"


class NewCdswRunner(CdswRunnerBase):
    def __init__(self, config: NewCdswRunnerConfig):
        super().__init__()
        self.cdsw_runner_config = config

        # Dynamic fields
        self.job_config: CdswJobConfig
        self.command_type = None

    # TODO Rename later
    def begin(self):
        self.job_config: CdswJobConfig = CdswJobConfigReader.read_from_file(config.job_config_file)

        if self.cdsw_runner_config.command_type != self.job_config.command_type:
            raise ValueError(
                "Specified command line command type is different than job's command type. CLI: {}, Job definition: {}".format(
                    args.cmd_type, self.job_config.command_type
                )
            )
        self.command_type = self.job_config.command_type
        self.dry_run = self.cdsw_runner_config.dry_run

        setup_result: CdswSetupResult = CdswSetup.initial_setup(mandatory_env_vars=self.job_config.mandatory_env_vars)
        self.start(setup_result, None)

    def start(self, setup_result: CdswSetupResult, cdsw_runner_script_path: str):
        self.start_common(setup_result, cdsw_runner_script_path)
        self._execute_runs()

    def _execute_runs(self):
        runs: List[CdswRun] = self.job_config.runs
        for run in runs:
            self.command_data_filename = run.email_settings
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
        if not run.drive_api_upload_settings:
            LOG.info("Google Drive upload settings is not defined for run: %s", run.name)
            return
        if not run.drive_api_upload_settings.enabled:
            LOG.info("Google Drive upload is disabled for run: %s", run.name)
            return

        drive_filename = run.drive_api_upload_settings.file_name
        if not self.dry_run:
            drive_api_file: DriveApiFile = self.upload_command_data_to_drive(self.command_type, drive_filename)
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


if __name__ == "__main__":
    start_time = time.time()

    args, parser = ArgParser.parse_args()
    end_time = time.time()
    config = NewCdswRunnerConfig(parser, args)
    cdsw_runner = NewCdswRunner(config)
    cdsw_runner.begin()

    LOG.info("Execution of script took %d seconds", end_time - start_time)
