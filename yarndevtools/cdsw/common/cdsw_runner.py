import inspect
import logging
import os
import time
from argparse import ArgumentParser
from enum import Enum
from typing import List, Tuple

from googleapiwrapper.google_drive import DriveApiFile
from pythoncommons.constants import ExecutionMode
from pythoncommons.date_utils import DateUtils
from pythoncommons.file_utils import FileUtils, FindResultType
from pythoncommons.logging_setup import SimpleLoggingSetupConfig, SimpleLoggingSetup
from pythoncommons.os_utils import OsUtils
from pythoncommons.process import SubprocessCommandRunner
from pythoncommons.project_utils import ProjectUtils

from yarndevtools.cdsw.common.cdsw_common import (
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
from yarndevtools.cdsw.common.cdsw_config import CdswJobConfigReader, CdswJobConfig, CdswRun
from yarndevtools.cdsw.common.constants import CdswEnvVar, BranchComparatorEnvVar
from yarndevtools.cdsw.common.restarter import Restarter
from yarndevtools.common.shared_command_utils import CommandType, RepoType
from yarndevtools.constants import YARNDEVTOOLS_MODULE_NAME

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
    def __init__(self, parser, args, config_reader: CdswConfigReaderAdapter = None):
        self._validate_args(parser, args)
        self.full_cmd: str = OsUtils.determine_full_command_filtered(filter_password=True)
        self.execution_mode = self.determine_execution_mode(args)
        self.job_config_file = self._determine_job_config_file_location(args)
        self._parse_command_type(args)
        self.dry_run = args.dry_run
        self.config_reader = config_reader

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
        expected_filename = f"{self.command_type.output_dir}_job_config.py"
        file_names = [os.path.basename(f) for f in file_paths]
        if expected_filename not in file_names:
            raise ValueError(
                "Auto-discovery failed for command '{}'. Expected file path: {}, Actual files found: {}".format(
                    self.command_type, expected_filename, file_paths
                )
            )
        return expected_filename

    def _parse_command_type(self, args):
        try:
            self.command_type = CommandType.by_real_name(args.cmd_type)
        except ValueError:
            pass  # Fallback to output_dir_name
        try:
            self.command_type = CommandType.by_output_dir_name(args.cmd_type)
        except ValueError:
            raise ValueError("Invalid command type specified! Possible values are: {}".format(POSSIBLE_COMMAND_TYPES))

    def _validate_args(self, parser, args):
        if hasattr(args, "config_file"):
            self.config_file = args.config_file
        if hasattr(args, "config_dir"):
            self.config_dir = args.config_dir

        if not self.config_file and not self.config_dir:
            parser.error("Either config file (--config-file) or config dir (--config-dir) need to be provided!")

    @staticmethod
    def determine_execution_mode(args):
        # If there's no --config-file specified, it means auto-discovery
        if not hasattr(args, "config_file"):
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
        self.start_date_str = None
        self.common_mail_config = CommonMailConfig()
        self._setup_google_drive()
        self.cdsw_runner_config = config
        self.dry_run = config.dry_run
        self.job_config: CdswJobConfig = config.config_reader.read_from_file(config.job_config_file)
        self.command_type = self._determine_command_type()

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
        setup_result: CdswSetupResult = CdswSetup.initial_setup(mandatory_env_vars=self.job_config.mandatory_env_vars)
        LOG.info("Setup result: %s", setup_result)
        self._execute_preparation_steps(setup_result)
        self.start_date_str = (
            self.current_date_formatted()
        )  # TODO Is this date the same as in RegularVariables.BUILT_IN_VARIABLES?

        for run in self.job_config.runs:
            self._execute_yarn_dev_tools(run)
            self._execute_command_data_zipper()
            drive_link_html_text = self._upload_command_data_to_google_drive_if_required(run)
            self._send_email_if_required(run, drive_link_html_text)

    def _execute_yarn_dev_tools(self, run: CdswRun):
        args = run.yarn_dev_tools_arguments
        args_as_string = " ".join(args)
        self.execute_yarndevtools_script(args_as_string)

    def _execute_command_data_zipper(self):
        self.execute_command_data_zipper(self.command_type, debug=True)

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
                FileUtils.create_new_dir(CommonDirs.HADOOP_CLOUDERA_BASEDIR)
                FileUtils.change_cwd(CommonDirs.HADOOP_CLOUDERA_BASEDIR)
                os.system("git init")
                self.execute_clone_upstream_repos_script(setup_result.basedir)

    def _setup_google_drive(self):
        if OsUtils.is_env_var_true(CdswEnvVar.ENABLE_GOOGLE_DRIVE_INTEGRATION.value, default_val=True):
            self.drive_cdsw_helper = GoogleDriveCdswHelper()
        else:
            self.drive_cdsw_helper = None

    def start_common(self, setup_result: CdswSetupResult, cdsw_runner_script_path: str):
        LOG.info("Starting CDSW runner...")
        LOG.info("Setup result: %s", setup_result)
        self.cdsw_runner_script_path = cdsw_runner_script_path
        self.start_date_str = (
            self.current_date_formatted()
        )  # TODO Is this the same as in RegularVariables.BUILT_IN_VARIABLES?
        if OsUtils.is_env_var_true(CdswEnvVar.RESTART_PROCESS_WHEN_REQUIREMENTS_INSTALLED.value, default_val=False):
            Restarter.restart_execution(self.cdsw_runner_script_path)

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
            SubprocessCommandRunner.run_and_follow_stdout_stderr(
                cmd, stdout_logger=CMD_LOG, exit_on_nonzero_exitcode=True
            )

    @staticmethod
    def current_date_formatted():
        return DateUtils.get_current_datetime()

    def execute_command_data_zipper(self, command_type: CommandType, debug=False, ignore_filetypes: str = "java js"):
        debug_mode = "--debug" if debug else ""
        self.execute_yarndevtools_script(
            f"{debug_mode} "
            f"{CommandType.ZIP_LATEST_COMMAND_DATA.name} {command_type.name} "
            f"--dest_dir /tmp "
            f"--ignore-filetypes {ignore_filetypes}"
        )

    def upload_command_data_to_drive(self, cmd_type: CommandType, drive_filename: str) -> DriveApiFile:
        output_basedir = ProjectUtils.get_output_basedir(YARNDEVTOOLS_MODULE_NAME)
        full_file_path_of_cmd_data = FileUtils.join_path(output_basedir, cmd_type.command_data_zip_name)
        return self.drive_cdsw_helper.upload(cmd_type, full_file_path_of_cmd_data, drive_filename)

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

    @staticmethod
    def get_filename(dir_name: str):
        # TODO Is this method used anymore?
        # Apparently, there is no chance to get the stackframe that called this method.
        # The 0th frame holds this method, though.
        # See file: cdsw_stacktrace_example.txt
        # Let's put the path together by hand
        stack = inspect.stack()
        LOG.debug("Discovered stack while getting filename: %s", stack)
        file_path = stack[0].filename
        rindex = file_path.rindex("cdsw" + os.sep)
        script_abs_path = file_path[:rindex] + f"cdsw{os.sep}{dir_name}{os.sep}cdsw_runner.py"
        if not os.path.exists(script_abs_path):
            raise ValueError(
                "Script should have existed under path: {}. "
                "Please double-check the code that assembles the path!".format(script_abs_path)
            )
        return script_abs_path

    @property
    def is_drive_integration_enabled(self):
        return self.drive_cdsw_helper is not None


if __name__ == "__main__":
    start_time = time.time()
    args, parser = ArgParser.parse_args()
    logging_config: SimpleLoggingSetupConfig = SimpleLoggingSetup.init_logger(
        project_name=YARNDEVTOOLS_MODULE_NAME,
        logger_name_prefix=YARNDEVTOOLS_MODULE_NAME,
        execution_mode=ExecutionMode.PRODUCTION,
        console_debug=args.debug,
        postfix=args.cmd_type,
        verbose_git_log=args.verbose,
    )
    LOG.info("Logging to files: %s", logging_config.log_file_paths)

    config = CdswRunnerConfig(parser, args, CdswConfigReaderAdapter())
    cdsw_runner = CdswRunner(config)
    cdsw_runner.start()

    end_time = time.time()
    LOG.info("Execution of script took %d seconds", end_time - start_time)
