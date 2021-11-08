#!/usr/bin/python

import sys
import logging
import os
import time
from logging.handlers import TimedRotatingFileHandler
from typing import Dict

from pythoncommons.constants import ExecutionMode
from pythoncommons.date_utils import DateUtils
from pythoncommons.file_utils import FileUtils
from pythoncommons.logging_setup import SimpleLoggingSetup, SimpleLoggingSetupConfig
from pythoncommons.os_utils import OsUtils
from pythoncommons.project_utils import ProjectUtils, ProjectRootDeterminationStrategy

from yarndevtools.commands.branchcomparator.branch_comparator import BranchComparator
from yarndevtools.commands.jenkinstestreporter.jenkins_test_reporter import JenkinsTestReporter
from yarndevtools.commands.send_latest_command_data_in_mail import SendLatestCommandDataInEmail
from yarndevtools.commands.unittestresultaggregator.unit_test_result_aggregator import UnitTestResultAggregator
from yarndevtools.commands.zip_latest_command_data import ZipLatestCommandData
from yarndevtools.argparser import ArgParser, CommandType
from yarndevtools.commands.backporter import Backporter
from yarndevtools.commands.format_patch_saver import FormatPatchSaver
from yarndevtools.commands.patch_saver import PatchSaver
from yarndevtools.commands.review_branch_creator import ReviewBranchCreator
from yarndevtools.commands.upstream_jira_patch_differ import UpstreamJiraPatchDiffer
from yarndevtools.commands.upstream_jira_umbrella_fetcher import UpstreamJiraUmbrellaFetcher
from yarndevtools.commands.upstream_pr_fetcher import UpstreamPRFetcher
from yarndevtools.common.shared_command_utils import YarnDevToolsEnvVar
from yarndevtools.constants import (
    PROJECT_NAME,
    ENV_HADOOP_DEV_DIR,
    ENV_CLOUDERA_HADOOP_ROOT,
    LOADED_ENV_DOWNSTREAM_DIR,
    LOADED_ENV_UPSTREAM_DIR,
    TRUNK,
    ORIGIN_TRUNK,
    GERRIT_REVIEWER_LIST,
    HADOOP_REPO_TEMPLATE,
    LATEST_DATA_ZIP_LINK_NAME,
    YARN_TASKS,
    JIRA_UMBRELLA_DATA,
    JIRA_PATCH_DIFFER,
    BRANCH_COMPARATOR,
    JENKINS_TEST_REPORTER,
    UNIT_TEST_RESULT_AGGREGATOR,
    YARNDEVTOOLS_MODULE_NAME,
)
from pythoncommons.git_wrapper import GitWrapper

__author__ = "Szilard Nemeth"
DEFAULT_BASE_BRANCH = TRUNK
LOG = logging.getLogger(__name__)
IGNORE_LATEST_SYMLINK_COMMANDS = {CommandType.ZIP_LATEST_COMMAND_DATA}


class YarnDevTools:
    def __init__(self, execution_mode: ExecutionMode = ExecutionMode.PRODUCTION):
        self.env = {}
        self.downstream_repo = None
        self.upstream_repo = None
        self.project_out_root = None
        self.yarn_patch_dir = None
        self.setup_dirs(execution_mode=execution_mode)
        self.init_repos()

    def setup_dirs(self, execution_mode: ExecutionMode = ExecutionMode.PRODUCTION):
        if execution_mode == ExecutionMode.PRODUCTION:
            strategy = ProjectRootDeterminationStrategy.SYS_PATH
        elif execution_mode == ExecutionMode.TEST:
            strategy = ProjectRootDeterminationStrategy.COMMON_FILE
        if YarnDevToolsEnvVar.PROJECT_DETERMINATION_STRATEGY.value in os.environ:
            env_value = os.environ[YarnDevToolsEnvVar.PROJECT_DETERMINATION_STRATEGY.value]
            LOG.info("Found specified project root determination strategy from env var: %s", env_value)
            strategy = ProjectRootDeterminationStrategy[env_value.upper()]
        LOG.info("Project root determination strategy is: %s", strategy)
        ProjectUtils.project_root_determine_strategy = strategy
        self.project_out_root = ProjectUtils.get_output_basedir(PROJECT_NAME)
        self.yarn_patch_dir = ProjectUtils.get_output_child_dir(YARN_TASKS)

    @property
    def jira_umbrella_data_dir(self):
        return ProjectUtils.get_output_child_dir(JIRA_UMBRELLA_DATA)

    @property
    def jira_patch_differ_dir(self):
        return ProjectUtils.get_output_child_dir(JIRA_PATCH_DIFFER)

    @property
    def branch_comparator_output_dir(self):
        return ProjectUtils.get_output_child_dir(BRANCH_COMPARATOR)

    @property
    def jenkins_test_reporter_output_dir(self):
        return ProjectUtils.get_output_child_dir(JENKINS_TEST_REPORTER)

    @property
    def unit_test_result_aggregator_output_dir(self):
        return ProjectUtils.get_output_child_dir(UNIT_TEST_RESULT_AGGREGATOR)

    def ensure_required_env_vars_are_present(self):
        upstream_hadoop_dir = OsUtils.get_env_value(ENV_HADOOP_DEV_DIR, None)
        downstream_hadoop_dir = OsUtils.get_env_value(ENV_CLOUDERA_HADOOP_ROOT, None)
        if not upstream_hadoop_dir:
            raise ValueError(f"Upstream Hadoop dir (env var: {ENV_HADOOP_DEV_DIR}) is not set!")
        if not downstream_hadoop_dir:
            raise ValueError(f"Downstream Hadoop dir (env var: {ENV_CLOUDERA_HADOOP_ROOT}) is not set!")

        # Verify if dirs are created
        FileUtils.verify_if_dir_is_created(downstream_hadoop_dir)
        FileUtils.verify_if_dir_is_created(upstream_hadoop_dir)

        self.env = {LOADED_ENV_DOWNSTREAM_DIR: downstream_hadoop_dir, LOADED_ENV_UPSTREAM_DIR: upstream_hadoop_dir}

    def init_repos(self):
        self.ensure_required_env_vars_are_present()
        self.downstream_repo = GitWrapper(self.env[LOADED_ENV_DOWNSTREAM_DIR])
        self.upstream_repo = GitWrapper(self.env[LOADED_ENV_UPSTREAM_DIR])

    def save_patch(self, args, parser=None):
        patch_saver = PatchSaver(args, self.upstream_repo, self.yarn_patch_dir, DEFAULT_BASE_BRANCH)
        return patch_saver.run()

    def create_review_branch(self, args, parser=None):
        review_branch_creator = ReviewBranchCreator(args, self.upstream_repo, DEFAULT_BASE_BRANCH, ORIGIN_TRUNK)
        review_branch_creator.run()

    def backport_c6(self, args, parser=None):
        mvn_cmd = "mvn clean install -Pdist -DskipTests -Pnoshade  -Dmaven.javadoc.skip=true"
        build_cmd = (
            "!! Remember to build project to verify the backported commit compiles !!"
            f"Run this command to build the project: {mvn_cmd}"
        )
        gerrit_push_cmd = (
            "Run this command to push to gerrit: "
            f"git push cauldron HEAD:refs/for/{args.downstream_branch}%{GERRIT_REVIEWER_LIST}"
        )
        post_commit_messages = [build_cmd, gerrit_push_cmd]

        downstream_base_ref = f"cauldron/{args.downstream_branch}"
        if "downstream_base_ref" in args and args.downstream_base_ref is not None:
            downstream_base_ref = args.downstream_base_ref
        backporter = Backporter(
            args,
            self.upstream_repo,
            self.downstream_repo,
            downstream_base_ref,
            post_commit_messages=post_commit_messages,
        )
        backporter.run()

    def upstream_pr_fetch(self, args, parser=None):
        remote_repo_url = HADOOP_REPO_TEMPLATE.format(user=args.github_username)
        upstream_pr_fetcher = UpstreamPRFetcher(args, remote_repo_url, self.upstream_repo, DEFAULT_BASE_BRANCH)
        upstream_pr_fetcher.run()

    def save_patches(self, args, parser=None):
        format_patch_saver = FormatPatchSaver(args, os.getcwd(), DateUtils.get_current_datetime())
        format_patch_saver.run()

    def diff_patches_of_jira(self, args, parser=None):
        """
        THIS SCRIPT ASSUMES EACH PROVIDED BRANCH WITH PARAMETERS (e.g. trunk, 3.2, 3.1) has the given commit committed
        Example workflow:
        1. git log --oneline trunk | grep YARN-10028
        * 13cea0412c1 - YARN-10028. Integrate the new abstract log servlet to the JobHistory server.
        Contributed by Adam Antal 24 hours ago) <Szilard Nemeth>

        2. git diff 13cea0412c1..13cea0412c1^ > /tmp/YARN-10028-trunk.diff
        3. git checkout branch-3.2
        4. git apply ~/Downloads/YARN-10028.branch-3.2.001.patch
        5. git diff > /tmp/YARN-10028-branch-32.diff
        6. diff -Bibw /tmp/YARN-10028-trunk.diff /tmp/YARN-10028-branch-32.diff
        :param args:
        :return:
        """
        patch_differ = UpstreamJiraPatchDiffer(args, self.upstream_repo, self.jira_patch_differ_dir)
        patch_differ.run()

    def fetch_jira_umbrella_data(self, args, parser=None):
        jira_umbrella_fetcher = UpstreamJiraUmbrellaFetcher(
            args, self.upstream_repo, self.downstream_repo, self.jira_umbrella_data_dir, DEFAULT_BASE_BRANCH
        )
        FileUtils.create_symlink_path_dir(
            CommandType.FETCH_JIRA_UMBRELLA_DATA.session_link_name,
            jira_umbrella_fetcher.config.umbrella_result_basedir,
            self.project_out_root,
        )
        jira_umbrella_fetcher.run()

    def branch_comparator(self, args, parser=None):
        branch_comparator = BranchComparator(
            args, self.downstream_repo, self.upstream_repo, self.branch_comparator_output_dir
        )
        FileUtils.create_symlink_path_dir(
            CommandType.BRANCH_COMPARATOR.session_link_name, branch_comparator.config.output_dir, self.project_out_root
        )
        branch_comparator.run()

    def zip_latest_command_data(self, args, parser=None):
        zip_latest_cmd_data = ZipLatestCommandData(args, yarn_dev_tools.project_out_root)
        zip_latest_cmd_data.run()

    def send_latest_command_data(self, args, parser=None):
        file_to_send = FileUtils.join_path(yarn_dev_tools.project_out_root, LATEST_DATA_ZIP_LINK_NAME)
        send_latest_cmd_data = SendLatestCommandDataInEmail(args, file_to_send)
        send_latest_cmd_data.run()

    def fetch_send_jenkins_test_report(self, args, parser=None):
        jenkins_test_reporter = JenkinsTestReporter(args, self.jenkins_test_reporter_output_dir)
        jenkins_test_reporter.run()

    def unit_test_result_aggregator(self, args, parser=None):
        ut_results_aggregator = UnitTestResultAggregator(args, parser, self.unit_test_result_aggregator_output_dir)
        FileUtils.create_symlink_path_dir(
            CommandType.UNIT_TEST_RESULT_AGGREGATOR.session_link_name,
            ut_results_aggregator.config.session_dir,
            self.project_out_root,
        )
        ut_results_aggregator.run()


if __name__ == "__main__":
    start_time = time.time()

    # TODO Revisit all exception handling: ValueError vs. exit() calls
    # Methods should throw exceptions, exit should be handled in this method
    yarn_dev_tools = YarnDevTools()

    # Parse args, commands will be mapped to YarnDevTools functions in ArgParser.parse_args
    args, parser = ArgParser.parse_args(yarn_dev_tools)
    logging_config: SimpleLoggingSetupConfig = SimpleLoggingSetup.init_logger(
        project_name=PROJECT_NAME,
        logger_name_prefix=YARNDEVTOOLS_MODULE_NAME,
        execution_mode=ExecutionMode.PRODUCTION,
        console_debug=args.debug,
        postfix=args.command,
        repos=[yarn_dev_tools.upstream_repo.repo, yarn_dev_tools.downstream_repo.repo],
        verbose_git_log=args.verbose,
    )

    cmd_type = CommandType.from_str(args.command)
    if cmd_type not in IGNORE_LATEST_SYMLINK_COMMANDS:
        for log_level, log_file_path in logging_config.log_file_paths.items():
            log_level_name = logging.getLevelName(log_level)
            link_name = cmd_type.log_link_name + "-" + log_level_name
            FileUtils.create_symlink_path_dir(link_name, log_file_path, yarn_dev_tools.project_out_root)
    else:
        LOG.info(f"Skipping to re-create symlink as command is: {args.command}")

    # Call the handler function
    args.func(args, parser=parser)

    end_time = time.time()
    LOG.info("Execution of script took %d seconds", end_time - start_time)
