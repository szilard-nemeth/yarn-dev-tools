import logging
import os
import time
from argparse import ArgumentParser
from enum import Enum
from typing import List, Tuple

from googleapiwrapper.google_drive import DriveApiFile
from pythoncommons.date_utils import DateUtils
from pythoncommons.file_utils import FileUtils, FindResultType
from pythoncommons.os_utils import OsUtils
from pythoncommons.process import SubprocessCommandRunner

from yarndevtools.cdsw.cdsw_common import (
    CdswSetupResult,
    CdswSetup,
    CommonDirs,
    GoogleDriveCdswHelper,
    CMD_LOG,
    BASHX,
    PY3,
    CommonFiles,
    MAIL_ADDR_YARN_ENG_BP,
    CommonMailConfig,
)
from yarndevtools.cdsw.cdsw_config import CdswJobConfigReader, CdswJobConfig, DriveApiUploadSettings, EmailSettings
from yarndevtools.cdsw.constants import CdswEnvVar, BranchComparatorEnvVar
from yarndevtools.common.shared_command_utils import CommandType, RepoType

LOG = logging.getLogger(__name__)
POSSIBLE_COMMAND_TYPES = [e.real_name for e in CommandType] + [e.output_dir_name for e in CommandType]


class ConfigMode(Enum):
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
            choices=POSSIBLE_COMMAND_TYPES,
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
        parser.add_argument("--config-dir", type=str, help="Full path to the directory of the configs")

        args = parser.parse_args()
        if args.verbose:
            print("Args: " + str(args))
        return args, parser


class CdswConfigReaderAdapter:
    def read_from_file(self, file: str):
        return CdswJobConfigReader.read_from_file(file)


class CdswRunnerConfig:
    def __init__(
        self,
        parser,
        args,
        config_reader: CdswConfigReaderAdapter = None,
        hadoop_cloudera_basedir=CommonDirs.HADOOP_CLOUDERA_BASEDIR,
    ):
        self._validate_args(parser, args)
        self.command_type = self._parse_command_type(args)
        self.full_cmd: str = OsUtils.determine_full_command_filtered(filter_password=True)
        self.execution_mode = self.determine_execution_mode(args)
        self.job_config_file = self._determine_job_config_file_location(args)
        self.dry_run = args.dry_run
        self.config_reader = config_reader
        self.hadoop_cloudera_basedir = hadoop_cloudera_basedir

    def _determine_job_config_file_location(self, args):
        if self.execution_mode == ConfigMode.SPECIFIED_CONFIG_FILE:
            return args.config_file
        elif self.execution_mode == ConfigMode.AUTO_DISCOVERY:
            LOG.info("Trying to discover config file for command: %s", self.command_type)
            return self._discover_config_file()

    def _discover_config_file(self):
        file_paths = FileUtils.find_files(
            self.config_dir,
            find_type=FindResultType.FILES,
            regex=".*\\.py",
            single_level=True,
            full_path_result=True,
        )
        expected_filename = f"{self.command_type.real_name}_job_config.py"
        file_names = [os.path.basename(f) for f in file_paths]
        if expected_filename not in file_names:
            raise ValueError(
                "Auto-discovery failed for command '{}'. Expected file path: {}, Actual files found: {}".format(
                    self.command_type, expected_filename, file_paths
                )
            )
        return FileUtils.join_path(self.config_dir, expected_filename)

    @staticmethod
    def _parse_command_type(args):
        try:
            command_type = CommandType.by_real_name(args.cmd_type)
            if command_type:
                return command_type
        except ValueError:
            pass  # Fallback to output_dir_name
        try:
            command_type = CommandType.by_output_dir_name(args.cmd_type)
            if command_type:
                return command_type
        except ValueError:
            pass
        try:
            command_type = CommandType[args.cmd_type]
            if command_type:
                return command_type
        except Exception:
            raise ValueError(
                "Invalid command type specified: {}. Possible values are: {}".format(
                    args.cmd_type, POSSIBLE_COMMAND_TYPES
                )
            )

    def _validate_args(self, parser, args):
        self.config_file = self.config_dir = None
        if hasattr(args, "config_file") and args.config_file:
            self.config_file = args.config_file
        if hasattr(args, "config_dir") and args.config_dir:
            self.config_dir = args.config_dir

        if not self.config_file and not self.config_dir:
            parser.error("Either config file (--config-file) or config dir (--config-dir) need to be provided!")

    @staticmethod
    def determine_execution_mode(args):
        # If there's no --config-file specified, it means auto-discovery
        if not hasattr(args, "config_file") or not args.config_file:
            LOG.info("Config file not specified! Activated mode: %s", ConfigMode.AUTO_DISCOVERY)
            return ConfigMode.AUTO_DISCOVERY
        return ConfigMode.SPECIFIED_CONFIG_FILE

    def __str__(self):
        return f"Full command: {self.full_cmd}\n"


class CdswRunner:
    def __init__(self, config: CdswRunnerConfig):
        self.executed_commands = []
        self.google_drive_uploads: List[
            Tuple[CommandType, str, DriveApiFile]
        ] = []  # Tuple of: (command_type, drive_filename, drive_api_file)
        self.common_mail_config = CommonMailConfig()
        self._setup_google_drive()
        self.cdsw_runner_config = config
        self.dry_run = config.dry_run

        # Dynamic fields
        self.job_config = None
        self.command_type = None
        self.output_basedir = None

    def _determine_command_type(self):
        if self.cdsw_runner_config.command_type != self.job_config.command_type:
            raise ValueError(
                "Specified command line command type is different than job's command type. CLI: {}, Job definition: {}".format(
                    self.cdsw_runner_config.command_type, self.job_config.command_type
                )
            )
        return self.job_config.command_type

    def start(self):
        LOG.info("Starting CDSW runner...")
        setup_result: CdswSetupResult = CdswSetup.initial_setup()
        LOG.info("Setup result: %s", setup_result)
        self.job_config: CdswJobConfig = self.cdsw_runner_config.config_reader.read_from_file(
            self.cdsw_runner_config.job_config_file
        )
        self.command_type = self._determine_command_type()
        self.output_basedir = setup_result.output_basedir
        self._execute_preparation_steps(setup_result)

        failed = False
        for run in self.job_config.runs:
            try:
                self.execute_yarndevtools_script(" ".join(run.yarn_dev_tools_arguments))
            except Exception:
                failed = True
                LOG.exception("Failed to execute run. Details: {}".format(run), exc_info=True)
            self._post_run_actions(run)
            if failed:
                LOG.info("Previous run failed, not going to continue processing more runs.")
                break

    def _post_run_actions(self, run):
        # TODO CDSW-new Introduce optional env var to remove all log files, log_file_paths are determined in: CdswSetup.initial_setup
        if self.command_type.session_based:
            self.execute_command_data_zipper(self.command_type, debug=True)
            drive_link_html_text = self._upload_command_data_to_google_drive_if_required(
                run.name, run.drive_api_upload_settings
            )
            self._send_email_if_required(run.name, run.email_settings, drive_link_html_text)
            # TODO CDSW-new now the zip file can be removed
            # _get_command_data_zip_file_path

    def _upload_command_data_to_google_drive_if_required(self, run_name: str, settings: DriveApiUploadSettings):
        if not self.is_drive_integration_enabled:
            LOG.info(
                "Google Drive integration is disabled with env var '%s'!",
                CdswEnvVar.ENABLE_GOOGLE_DRIVE_INTEGRATION.value,
            )
            return
        if not settings:
            LOG.info("Google Drive upload settings is not defined for run: %s", run_name)
            return
        if not settings.enabled:
            LOG.info("Google Drive upload is disabled for run: %s", run_name)
            return

        drive_filename = settings.file_name
        if not self.dry_run:
            local_file = self._get_command_data_zip_file_path(self.command_type)
            drive_api_file: DriveApiFile = self.upload_command_data_to_drive(
                self.command_type, local_file, drive_filename
            )
            self.google_drive_uploads.append((self.command_type, drive_filename, drive_api_file))
            return f'<a href="{drive_api_file.link}">Command data file: {drive_filename}</a>'
        else:
            LOG.info(
                "[DRY-RUN] Would upload file for command type '%s' to Google Drive with name '%s'",
                self.command_type,
                drive_filename,
            )
            return f'<a href="dummy_link">Command data file: {drive_filename}</a>'

    def _send_email_if_required(self, run_name: str, settings: EmailSettings, drive_link_html_text: str or None):
        if not settings:
            LOG.info("Email settings is not defined for run: %s", run_name)
            return
        if not settings.enabled:
            LOG.info("Email sending is disabled for run: %s", run_name)
            return

        kwargs = {
            "attachment_filename": settings.attachment_file_name,
            "email_body_file": settings.email_body_file_from_command_data,
            "send_attachment": True,
        }
        if drive_link_html_text:
            kwargs["prepend_text_to_email_body"] = drive_link_html_text

        LOG.debug("kwargs for email: %s", kwargs)
        self.send_latest_command_data_in_email(
            sender=settings.sender,
            subject=settings.subject,
            **kwargs,
        )

    def _execute_preparation_steps(self, setup_result):
        if self.command_type == CommandType.JIRA_UMBRELLA_DATA_FETCHER:
            self.execute_clone_downstream_repos_script(setup_result.basedir)
            self.execute_clone_upstream_repos_script(setup_result.basedir)
        elif self.command_type == CommandType.BRANCH_COMPARATOR:
            repo_type_env = OsUtils.get_env_value(
                BranchComparatorEnvVar.BRANCH_COMP_REPO_TYPE.value, RepoType.DOWNSTREAM.value
            )
            repo_type: RepoType = RepoType[repo_type_env.upper()]

            if repo_type == RepoType.DOWNSTREAM:
                self.execute_clone_downstream_repos_script(setup_result.basedir)
            elif repo_type == RepoType.UPSTREAM:
                # If we are in upstream mode, make sure downstream dir exist
                # Currently, yarndevtools requires both repos to be present when initializing.
                # BranchComparator is happy with one single repository, upstream or downstream, exclusively.
                # Git init the other repository so everything will be alright
                FileUtils.create_new_dir(self.cdsw_runner_config.hadoop_cloudera_basedir, fail_if_created=False)
                FileUtils.change_cwd(self.cdsw_runner_config.hadoop_cloudera_basedir)
                os.system("git init")
                self.execute_clone_upstream_repos_script(setup_result.basedir)

    def _setup_google_drive(self):
        if OsUtils.is_env_var_true(CdswEnvVar.ENABLE_GOOGLE_DRIVE_INTEGRATION.value, default_val=True):
            self.drive_cdsw_helper = self.create_google_drive_cdsw_helper()
        else:
            self.drive_cdsw_helper = None

    def create_google_drive_cdsw_helper(self):
        return GoogleDriveCdswHelper()

    def execute_clone_downstream_repos_script(self, basedir):
        script = os.path.join(basedir, "clone_downstream_repos.sh")
        cmd = f"{BASHX} {script}"
        self._run_command(cmd)

    def execute_clone_upstream_repos_script(self, basedir):
        script = os.path.join(basedir, "clone_upstream_repos.sh")
        cmd = f"{BASHX} {script}"
        self._run_command(cmd)

    def execute_yarndevtools_script(self, script_args):
        cmd = f"{PY3} {CommonFiles.YARN_DEV_TOOLS_SCRIPT} {script_args}"
        self._run_command(cmd)

    def _run_command(self, cmd):
        self.executed_commands.append(cmd)
        if self.dry_run:
            LOG.info("[DRY-RUN] Would run command: %s", cmd)
        else:
            process = SubprocessCommandRunner.run_and_follow_stdout_stderr(
                cmd, stdout_logger=CMD_LOG, exit_on_nonzero_exitcode=False
            )
            if process.returncode != 0:
                raise ValueError("Process execution failed, command was: {}".format(cmd))

    @staticmethod
    def current_date_formatted():
        return DateUtils.get_current_datetime()

    def execute_command_data_zipper(self, command_type: CommandType, debug=False, ignore_filetypes: str = "java js"):
        # TODO CDSW-new add argument to zipper to remove zipped origin files
        debug_mode = "--debug" if debug else ""
        self.execute_yarndevtools_script(
            f"{debug_mode} "
            f"{CommandType.ZIP_LATEST_COMMAND_DATA.name} {command_type.name} "
            f"--dest_dir /tmp "
            f"--ignore-filetypes {ignore_filetypes}"
        )

    def upload_command_data_to_drive(self, cmd_type: CommandType, local_file: str, drive_filename: str) -> DriveApiFile:
        return self.drive_cdsw_helper.upload(cmd_type, local_file, drive_filename)

    def _get_command_data_zip_file_path(self, cmd_type):
        full_file_path_of_cmd_data = FileUtils.join_path(self.output_basedir, cmd_type.command_data_zip_name)
        return full_file_path_of_cmd_data

    def send_latest_command_data_in_email(
        self,
        sender,
        subject,
        recipients=None,
        attachment_filename=None,
        email_body_file: str = None,
        prepend_text_to_email_body: str = None,
        send_attachment: bool = True,
    ):
        if not recipients:
            recipients = self.determine_recipients()
        attachment_filename_val = f"{attachment_filename}" if attachment_filename else ""
        email_body_file_param = f"--file-as-email-body-from-zip {email_body_file}" if email_body_file else ""
        email_body_prepend_param = (
            f"--prepend_email_body_with_text '{prepend_text_to_email_body}'" if prepend_text_to_email_body else ""
        )
        send_attachment_param = "--send-attachment" if send_attachment else ""
        self.execute_yarndevtools_script(
            f"--debug {CommandType.SEND_LATEST_COMMAND_DATA.name} "
            f"{self.common_mail_config.as_arguments()}"
            f'--subject "{subject}" '
            f'--sender "{sender}" '
            f'--recipients "{recipients}" '
            f"--attachment-filename {attachment_filename_val} "
            f"{email_body_file_param} "
            f"{email_body_prepend_param} "
            f"{send_attachment_param}"
        )

    @staticmethod
    def determine_recipients(default_recipients=MAIL_ADDR_YARN_ENG_BP):
        recipients_env = OsUtils.get_env_value(CdswEnvVar.MAIL_RECIPIENTS.value)
        if recipients_env:
            return recipients_env
        return default_recipients

    @property
    def is_drive_integration_enabled(self):
        return self.drive_cdsw_helper is not None


if __name__ == "__main__":
    start_time = time.time()
    args, parser = ArgParser.parse_args()
    # TODO Temporarily removed
    # ProjectUtils.get_output_basedir(CDSW_PROJECT)
    # logging_config: SimpleLoggingSetupConfig = SimpleLoggingSetup.init_logger(
    #     project_name=CDSW_PROJECT,
    #     logger_name_prefix=CDSW_PROJECT,
    #     execution_mode=ExecutionMode.PRODUCTION,
    #     console_debug=args.debug,
    #     postfix=args.cmd_type,
    #     verbose_git_log=args.verbose,
    # )
    # LOG.info("Logging to files: %s", logging_config.log_file_paths)

    config = CdswRunnerConfig(parser, args, CdswConfigReaderAdapter())
    cdsw_runner = CdswRunner(config)
    cdsw_runner.start()

    end_time = time.time()
    LOG.info("Execution of script took %d seconds", end_time - start_time)
