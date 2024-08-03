#!/usr/bin/env python3
import os
import sys
from argparse import ArgumentParser

from cdswjoblauncher.cdsw.libreloader.reload_dependencies import Reloader
from pythoncommons.file_utils import FileUtils
from pythoncommons.os_utils import OsUtils

from yarndevtools.cdsw.constants import (
    BranchComparatorEnvVar,
    JiraUmbrellaFetcherEnvVar,
    UnitTestResultFetcherEnvVar,
    UnitTestResultAggregatorEmailEnvVar,
    ReviewSheetBackportUpdaterEnvVar,
    ReviewSyncEnvVar,
)
from yarndevtools.common.shared_command_utils import CommandType, YarnDevToolsEnvVar
from yarndevtools.constants import (
    YARNDEVTOOLS_MODULE_NAME,
    CDSW_JOB_LAUNCHER_MODULE_ROOT,
    YARNDEVTOOLS_MAIN_SCRIPT_NAME,
)

# THESE FUNCTION DEFINITIONS AND CALL TO fix_pythonpast MUST PRECEDE THE IMPORT OF libreloader: from libreloader import reload_dependencies
# TODO cdsw-separation same as CdswEnvVar.PYTHONPATH --> Migrate
PYTHONPATH_ENV_VAR = "PYTHONPATH"
MAIL_ADDR_YARN_ENG_BP = "yarn_eng_bp@cloudera.com"
POSSIBLE_COMMAND_TYPES = [e.real_name for e in CommandType] + [e.output_dir_name for e in CommandType]


class CommonDirs:
    CDSW_BASEDIR = FileUtils.join_path("home", "cdsw")
    HADOOP_UPSTREAM_BASEDIR = FileUtils.join_path(CDSW_BASEDIR, "repos", "apache", "hadoop")
    HADOOP_CLOUDERA_BASEDIR = FileUtils.join_path(CDSW_BASEDIR, "repos", "cloudera", "hadoop")


def append_arg(a):
    sys.argv.append(a)


def append_arg_and_value(a, v):
    sys.argv.append(a)
    sys.argv.append(v)


class ArgParser:
    @staticmethod
    def parse_args():
        parser = ArgumentParser()
        parser.add_argument(
            "cmd_type",
            type=str,
            choices=POSSIBLE_COMMAND_TYPES,
            help="Type of command.",
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            dest="verbose",
            default=False,
            required=False,
            help="More verbose log (including gitpython verbose logs)",
        )

        args = parser.parse_args()
        if args.verbose:
            print("Args: " + str(args))
        return args, parser


class Config:
    def __init__(
        self,
        parser,
        args,
    ):
        self._validate_args(parser, args)
        self.command_type = self._parse_command_type(args)

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
        pass


def get_valid_env_vars(config):
    command_to_env_var_class = {
        CommandType.JIRA_UMBRELLA_DATA_FETCHER: JiraUmbrellaFetcherEnvVar,
        CommandType.BRANCH_COMPARATOR: BranchComparatorEnvVar,
        CommandType.UNIT_TEST_RESULT_FETCHER: UnitTestResultFetcherEnvVar,
        CommandType.UNIT_TEST_RESULT_AGGREGATOR: UnitTestResultAggregatorEmailEnvVar,
        CommandType.REVIEW_SHEET_BACKPORT_UPDATER: ReviewSheetBackportUpdaterEnvVar,
        CommandType.REVIEWSYNC: ReviewSyncEnvVar,
    }
    enum_type = command_to_env_var_class[config.command_type]
    valid_env_vars = [e.value for e in enum_type]
    return valid_env_vars


def prepare_args_for_cdsw_runner(config, valid_env_vars):
    append_arg(YARNDEVTOOLS_MODULE_NAME)
    append_arg_and_value("--command-type-real-name", config.command_type.real_name)
    append_arg_and_value("--command-type-name", config.command_type.name)
    append_arg_and_value("--command-type-zip-name", config.command_type.command_data_zip_name)
    if config.command_type.session_based:
        append_arg("--command-type-session-based")
    append_arg_and_value("--command-type-valid-env-vars", " ".join(valid_env_vars))
    append_arg_and_value("--default-email-recipients", MAIL_ADDR_YARN_ENG_BP)
    append_arg_and_value("--module-name", YARNDEVTOOLS_MODULE_NAME)
    append_arg_and_value("--main-script-name", YARNDEVTOOLS_MAIN_SCRIPT_NAME)
    append_arg_and_value("--job-preparation-callback", "JobPreparation.execute")
    append_arg_and_value(
        "--env", f"{YarnDevToolsEnvVar.ENV_CLOUDERA_HADOOP_ROOT.value}={CommonDirs.HADOOP_CLOUDERA_BASEDIR}"
    )
    append_arg_and_value("--env", f"{YarnDevToolsEnvVar.ENV_HADOOP_DEV_DIR.value}={CommonDirs.HADOOP_UPSTREAM_BASEDIR}")

    # Set module version if yarndevtools branch is defined.
    # initial-cdsw-setup.sh and install-requirements.sh should be in sync for yarndevtools version
    if YarnDevToolsEnvVar.YARNDEVTOOLS_BRANCH.value in os.environ:
        branch = OsUtils.get_env_value(YarnDevToolsEnvVar.YARNDEVTOOLS_BRANCH.value, default_value=None)
        if branch:
            # TODO cdsw-separation ugly as hell :(
            version = os.system(
                "wget -q -O - https://raw.githubusercontent.com/szilard-nemeth/yarn-dev-tools/master/pyproject.toml | "
                'grep -A2 "name = "yarn-dev-tools" | grep -m 1 version | '
                "tr -s "
                " | tr -d "
                "' | tr -d "
                '" | cut -d'
                " -f3"
            )
            OsUtils.set_env_value(YarnDevToolsEnvVar.YARNDEVTOOLS_MODULE_VERSION.value, str(version))

            append_arg_and_value("--env", f"{YarnDevToolsEnvVar.YARNDEVTOOLS_BRANCH.value}={branch}")
            append_arg_and_value("--env", f"{YarnDevToolsEnvVar.YARNDEVTOOLS_MODULE_VERSION.value}={version}")


def main():
    module_root = Reloader.get_python_module_root()
    cdsw_job_launcher_module_root = os.path.join(module_root, CDSW_JOB_LAUNCHER_MODULE_ROOT)
    cdsw_runner_path = os.path.join(cdsw_job_launcher_module_root, "cdsw", "cdsw_runner.py")
    print("CDSW job launcher module root is: %s", cdsw_job_launcher_module_root)
    print("CDSW runner path: %s", cdsw_runner_path)

    args, parser = ArgParser.parse_args()
    config = Config(parser, args)
    valid_env_vars = get_valid_env_vars(config)

    # Start the CDSW runner
    prepare_args_for_cdsw_runner(config, valid_env_vars)
    print("Arguments for CDSW runner: " + str(sys.argv))
    exec(open(cdsw_runner_path).read())


if __name__ == "__main__":
    main()
