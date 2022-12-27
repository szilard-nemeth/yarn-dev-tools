#!/usr/bin/python

import logging
import os
import time

from pythoncommons.constants import ExecutionMode
from pythoncommons.file_utils import FileUtils
from pythoncommons.git_wrapper import GitWrapper
from pythoncommons.logging_setup import SimpleLoggingSetup, SimpleLoggingSetupConfig
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils, ProjectRootDeterminationStrategy

from yarndevtools.argparser import ArgParser
from yarndevtools.common.shared_command_utils import YarnDevToolsEnvVar, CommandType
from yarndevtools.constants import (
    LOADED_ENV_DOWNSTREAM_DIR,
    LOADED_ENV_UPSTREAM_DIR,
    YARNDEVTOOLS_MODULE_NAME,
)

__author__ = "Szilard Nemeth"

from yarndevtools.yarn_dev_tools_config import YarnDevToolsConfig

LOG = logging.getLogger(__name__)
IGNORE_LATEST_SYMLINK_COMMANDS = {CommandType.ZIP_LATEST_COMMAND_DATA}


class YarnDevTools:
    def __init__(self, execution_mode: ExecutionMode = ExecutionMode.PRODUCTION):
        self.env = {}
        self.setup_dirs(execution_mode=execution_mode)
        self.init_repos()

    def setup_dirs(self, execution_mode: ExecutionMode = ExecutionMode.PRODUCTION):
        strategy = None
        if execution_mode == ExecutionMode.PRODUCTION:
            strategy = ProjectRootDeterminationStrategy.SYS_PATH
        elif execution_mode == ExecutionMode.TEST:
            strategy = ProjectRootDeterminationStrategy.COMMON_FILE
        if YarnDevToolsEnvVar.PROJECT_DETERMINATION_STRATEGY.value in os.environ:
            env_value = OsUtils.get_env_value(YarnDevToolsEnvVar.PROJECT_DETERMINATION_STRATEGY.value)
            LOG.info("Found specified project root determination strategy from env var: %s", env_value)
            strategy = ProjectRootDeterminationStrategy[env_value.upper()]
        if not strategy:
            raise ValueError("Unknown project root determination strategy!")
        LOG.info("Project root determination strategy is: %s", strategy)
        ProjectUtils.project_root_determine_strategy = strategy
        YarnDevToolsConfig.PROJECT_OUT_ROOT = ProjectUtils.get_output_basedir(
            YARNDEVTOOLS_MODULE_NAME, project_name_hint=YARNDEVTOOLS_MODULE_NAME
        )

    def ensure_required_env_vars_are_present(self):
        upstream_hadoop_dir = OsUtils.get_env_value(YarnDevToolsEnvVar.ENV_HADOOP_DEV_DIR.value, None)
        downstream_hadoop_dir = OsUtils.get_env_value(YarnDevToolsEnvVar.ENV_CLOUDERA_HADOOP_ROOT.value, None)
        if not upstream_hadoop_dir:
            raise ValueError(
                f"Upstream Hadoop dir (env var: {YarnDevToolsEnvVar.ENV_HADOOP_DEV_DIR.value}) is not set!"
            )
        if not downstream_hadoop_dir:
            raise ValueError(
                f"Downstream Hadoop dir (env var: {YarnDevToolsEnvVar.ENV_CLOUDERA_HADOOP_ROOT.value}) is not set!"
            )

        # Verify if dirs are created
        FileUtils.verify_if_dir_is_created(downstream_hadoop_dir)
        FileUtils.verify_if_dir_is_created(upstream_hadoop_dir)

        self.env = {LOADED_ENV_DOWNSTREAM_DIR: downstream_hadoop_dir, LOADED_ENV_UPSTREAM_DIR: upstream_hadoop_dir}

    def init_repos(self):
        self.ensure_required_env_vars_are_present()
        YarnDevToolsConfig.DOWNSTREAM_REPO = GitWrapper(self.env[LOADED_ENV_DOWNSTREAM_DIR])
        YarnDevToolsConfig.UPSTREAM_REPO = GitWrapper(self.env[LOADED_ENV_UPSTREAM_DIR])


def run():
    global args, cmd_type
    start_time = time.time()
    # TODO Revisit all exception handling: ValueError vs. exit() calls
    # Methods should throw exceptions, exit should be handled in this method
    YarnDevTools()
    # Parse args, commands will be mapped to YarnDevTools functions in ArgParser.parse_args
    args, parser = ArgParser.parse_args()

    # TODO use this value later with SimpleLoggingSetup.init_logger instead of passing bool flags
    # log_level = determine_logging_level(args)
    debug = getattr(args, "logging_debug", False)
    trace = getattr(args, "logging_trace", False)
    logging_config: SimpleLoggingSetupConfig = SimpleLoggingSetup.init_logger(
        project_name=YARNDEVTOOLS_MODULE_NAME,
        logger_name_prefix=YARNDEVTOOLS_MODULE_NAME,
        execution_mode=ExecutionMode.PRODUCTION,
        # TODO find 'console_debug' in project and rename
        console_debug=debug,
        trace=trace,
        postfix=args.command,
        repos=[YarnDevToolsConfig.UPSTREAM_REPO.repo, YarnDevToolsConfig.DOWNSTREAM_REPO.repo],
        verbose_git_log=args.verbose,
        with_trace_level=True,
    )
    # LOG.trace("test trace")
    LOG.info("Logging to files: %s", logging_config.log_file_paths)
    cmd_type = CommandType.from_str(args.command)
    if cmd_type not in IGNORE_LATEST_SYMLINK_COMMANDS:
        for log_level, log_file_path in logging_config.log_file_paths.items():
            log_level_name = logging.getLevelName(log_level)
            link_name = cmd_type.log_link_name + "-" + log_level_name
            FileUtils.create_symlink_path_dir(link_name, log_file_path, YarnDevToolsConfig.PROJECT_OUT_ROOT)
    else:
        LOG.info(f"Skipping to re-create symlink as command is: {args.command}")

    # Call the handler function
    args.func(args, parser=parser)
    end_time = time.time()
    LOG.info("Execution of script took %d seconds", end_time - start_time)


def determine_logging_level(args):
    log_levels = {
        logging.DEBUG: getattr(args, "logging_debug", False),
        # TODO
        # logging.TRACE: getattr(args, "logging_trace", False),
        logging.INFO: True,  # Info is always on
    }
    val = 99999
    for level_value, enabled in log_levels.items():
        if level_value < val and enabled:
            val = level_value

    return logging.getLevelName(val)


if __name__ == "__main__":
    run()
