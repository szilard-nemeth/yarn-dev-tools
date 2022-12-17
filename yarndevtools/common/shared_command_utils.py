import logging
from enum import Enum
from typing import List, Dict

from pythoncommons.email import EmailAccount, EmailConfig
from pythoncommons.file_utils import FileUtils
from pythoncommons.git_constants import ORIGIN
from pythoncommons.git_wrapper import GitLogLineFormat
from pythoncommons.html_utils import HtmlGenerator
from pythoncommons.object_utils import ListUtils, ObjUtils
from pythoncommons.process import CommandRunner

from yarndevtools.commands_common import (
    CommitData,
    BackportedJira,
    BackportedCommit,
    MatchAllJiraIdStrategy,
    JiraIdTypePreference,
    JiraIdChoosePreference,
)

from yarndevtools.constants import LATEST_DATA_ZIP_LINK_NAME, ANY_JIRA_ID_PATTERN

LOG = logging.getLogger(__name__)


class EnvVar(Enum):
    IGNORE_SMTP_AUTH_ERROR = "IGNORE_SMTP_AUTH_ERROR"


class YarnDevToolsTestEnvVar(Enum):
    FORCE_COLLECTING_ARTIFACTS = "FORCE_COLLECTING_ARTIFACTS"


class YarnDevToolsEnvVar(Enum):
    PROJECT_DETERMINATION_STRATEGY = "PROJECT_DETERMINATION_STRATEGY"
    ENV_CLOUDERA_HADOOP_ROOT = "CLOUDERA_HADOOP_ROOT"
    ENV_HADOOP_DEV_DIR = "HADOOP_DEV_DIR"


class RepoType(Enum):
    DOWNSTREAM = "downstream"
    UPSTREAM = "upstream"


class SharedCommandUtils:
    @staticmethod
    def ensure_remote_specified(branch):
        if ORIGIN not in branch:
            return f"{ORIGIN}/{branch}"
        return branch

    @staticmethod
    def find_commits_on_branches(
        branches, grep_intermediate_results_file, downstream_repo, jira_ids
    ) -> Dict[str, BackportedJira]:
        backported_jiras: Dict[str, BackportedJira] = {}
        for branch in branches:
            git_log_result = downstream_repo.log(
                SharedCommandUtils.ensure_remote_specified(branch), oneline_with_date=True
            )
            if len(jira_ids) > 100:
                for jira_ids_chunk in ListUtils.split_to_chunks(jira_ids, 100):
                    ret = SharedCommandUtils._run_git_egrep(
                        jira_ids_chunk, branch, backported_jiras, git_log_result, grep_intermediate_results_file
                    )
                    if ret == -1:
                        continue
            else:
                ret = SharedCommandUtils._run_git_egrep(
                    jira_ids, branch, backported_jiras, git_log_result, grep_intermediate_results_file
                )
                if ret == -1:
                    continue

        LOG.info("Found %d backported commits out of %d", len(backported_jiras), len(jira_ids))
        # Make sure that missing backports are added as BackportedJira objects
        for jira_id in jira_ids:
            if jira_id not in backported_jiras:
                LOG.debug("%s is not backported to any of the provided branches", jira_id)
                backported_jiras[jira_id] = BackportedJira(jira_id, [])
        return backported_jiras

    @staticmethod
    def _run_git_egrep(jira_ids, branch, backported_jiras, git_log_result, grep_intermediate_results_file):
        piped_jira_ids = SharedCommandUtils._prepare_jira_ids(jira_ids)
        # It's quite complex to grep for multiple jira IDs with gitpython, so let's rather call an external command
        cmd, output = SharedCommandUtils._run_egrep(
            git_log_result, grep_intermediate_results_file, piped_jira_ids, fail_on_error=False
        )
        if not output or len(output) == 0:
            return -1

        SharedCommandUtils._process_output(backported_jiras, branch, output)
        return 0

    @staticmethod
    def _prepare_jira_ids(jira_ids):
        # Do not allow partial matches, for example:
        # If Jira ID is 'YARN-1015' and there are commit messages like:
        # YARN-10157..., YARN-1015..., YARN-101...,
        # then all of the following will be matched instead of just finding 'YARN-10157':
        # YARN-10157
        # YARN-1015
        # YARN-101
        mod_jira_ids = [jid + "[^0-9]" for jid in jira_ids]
        piped_jira_ids = "|".join(mod_jira_ids)
        return piped_jira_ids.replace("\n", "")

    @staticmethod
    def _process_output(backported_jiras, branch, output):
        matched_downstream_commit_list = output.split("\n")
        if matched_downstream_commit_list:
            backported_commits = [
                BackportedCommit(
                    CommitData.from_git_log_str(
                        commit_str,
                        format=GitLogLineFormat.ONELINE_WITH_DATE,
                        jira_id_parse_strategy=MatchAllJiraIdStrategy(
                            type_preference=JiraIdTypePreference.UPSTREAM,
                            choose_preference=JiraIdChoosePreference.FIRST,
                            fallback_type=JiraIdTypePreference.DOWNSTREAM,
                        ),
                        pattern=ANY_JIRA_ID_PATTERN,
                    ),
                    [branch],
                )
                for commit_str in matched_downstream_commit_list
            ]
            LOG.info(
                "Identified %d backported commits on branch %s:\n%s",
                len(backported_commits),
                branch,
                "\n".join([f"{bc.commit_obj.as_oneline_string()}" for bc in backported_commits]),
            )

            for backported_commit in backported_commits:
                commit_obj = backported_commit.commit_obj
                jira_id = commit_obj.jira_id
                if jira_id not in backported_jiras:
                    backported_jiras[jira_id] = BackportedJira(jira_id, [backported_commit])
                else:
                    # TODO Consider using set data structure instead
                    if backported_commit not in backported_jiras[jira_id].commits:
                        backported_jiras[jira_id].add_backported_commit(backported_commit)
                    else:
                        backported_jiras[jira_id].extend_branches_by_hash(commit_obj.hash, backported_commit)

    @staticmethod
    def _run_egrep(git_log_result: List[str], file: str, grep_for: str, fail_on_error=False):
        return CommandRunner.egrep_with_cli(
            git_log_result,
            file,
            grep_for,
            escape_single_quotes=False,
            escape_double_quotes=True,
            fail_on_empty_output=False,
            fail_on_error=fail_on_error,
        )


class FullEmailConfig:
    def __init__(self, args, attachment_file: str = None, allow_empty_subject=False):
        mandatory_attrs = [
            ("account_user", "Email account user"),
            ("account_password", "Email account password"),
            ("smtp_server", "Email SMTP server"),
            ("smtp_port", "Email SMTP port"),
            ("sender", "Email sender"),
            ("recipients", "Email recipients"),
        ]
        all_attrs = []
        all_attrs.extend(mandatory_attrs)
        if not allow_empty_subject:
            all_attrs.append(("subject", "Email subject"))

        ObjUtils.ensure_all_attrs_present(
            args,
            all_attrs,
        )
        if not isinstance(args.recipients, list):
            raise ValueError("Email recipients should be a List[str]!")

        self.attachment_file = None
        if attachment_file:
            FileUtils.ensure_file_exists_and_readable(attachment_file)
            self.attachment_file = attachment_file
        self.email_account: EmailAccount = EmailAccount(args.account_user, args.account_password)
        self.email_conf: EmailConfig = EmailConfig(args.smtp_server, args.smtp_port, self.email_account)
        self.sender: str = args.sender
        self.recipients = args.recipients
        self.subject: str = args.subject if "subject" in args else None
        self.attachment_filename: str = args.attachment_filename if hasattr(args, "attachment_filename") else None

    def __str__(self):
        return (
            f"SMTP server: {self.email_conf.smtp_server}\n"
            f"SMTP port: {self.email_conf.smtp_port}\n"
            f"Account user: {self.email_account.user}\n"
            f"Recipients: {self.recipients}\n"
            f"Sender: {self.sender}\n"
            f"Subject: {self.subject}\n"
            f"Attachment file: {self.attachment_file}\n"
        )


class CommandType(Enum):
    SAVE_PATCH = ("save_patch", "yarn-tasks", False)
    CREATE_REVIEW_BRANCH = ("create_review_branch", "create-review-branch", False)
    BACKPORT = ("backport", "backport", False)
    UPSTREAM_PR_FETCH = ("upstream_pr_fetch", "upstream-pr-fetch", False)
    SAVE_DIFF_AS_PATCHES = ("save_diff_as_patches", "save-diff-as-patches", False)
    DIFF_PATCHES_OF_JIRA = ("diff_patches_of_jira", "jira-patch-differ", False)
    JIRA_UMBRELLA_DATA_FETCHER = (
        "jira_umbrella_data_fetcher",
        "jira-umbrella-data-fetcher",
        True,
        "latest-session-jira-umbrella-data-fetcher",
    )
    BRANCH_COMPARATOR = ("branch_comparator", "branch-comparator", True, "latest-session-branchcomparator")
    ZIP_LATEST_COMMAND_DATA = ("zip_latest_command_data", "zip-latest-command-data", False)
    SEND_LATEST_COMMAND_DATA = ("send_latest_command_data", "send-latest-command-data", False)
    UNIT_TEST_RESULT_FETCHER = ("unit_test_result_fetcher", "unit-test-result-fetcher", False)
    UNIT_TEST_RESULT_AGGREGATOR_DB = (
        "unit_test_result_aggregator",
        "unit-test-result-aggregator",
        True,
        "latest-session-unit-test-result-aggregator-db",
    )
    UNIT_TEST_RESULT_AGGREGATOR_EMAIL = (
        "unit_test_result_aggregator",
        "unit-test-result-aggregator",
        True,
        "latest-session-unit-test-result-aggregator-email",
    )

    UNIT_TEST_RESULT_AGGREGATOR_DB_CONNECTOR = (
        "unit_test_result_aggregator",
        "unit-test-result-aggregator",
        True,
        "latest-session-unit-test-result-aggregator-db-connector",
    )

    REVIEW_SHEET_BACKPORT_UPDATER = (
        "review_sheet_backport_updater",
        "review-sheet-backport-updater",
        True,
        "latest-session-review-sheet-backport-updater",
    )
    REVIEWSYNC = (
        "reviewsync",
        "reviewsync",
        True,
        "latest-session-reviewsync",
    )

    # TODO Unify value vs. output_dir_name: Using both causes confusion
    def __init__(self, value, output_dir_name, session_based: bool, session_link_name: str = ""):
        self.real_name = value
        self.session_based = session_based
        self.output_dir_name = output_dir_name

        if session_link_name:
            self.session_link_name = session_link_name
        else:
            self.session_link_name = f"latest-session-{value}"

        self.log_link_name = f"latest-log-{value}"
        self.command_data_name = f"latest-command-data-{value}"
        self.command_data_zip_name: str = f"{LATEST_DATA_ZIP_LINK_NAME}-{value}"

    @staticmethod
    def from_str(val):
        allowed_values = {ct.name: ct for ct in CommandType}
        return CommandType._validate(val, allowed_values, "Invalid enum key")

    @staticmethod
    def by_real_name(val):
        allowed_values = {ct.real_name: ct for ct in CommandType}
        return CommandType._validate(val, allowed_values, "Invalid enum value by real name")

    @staticmethod
    def by_output_dir_name(val):
        allowed_values = {ct.output_dir_name: ct for ct in CommandType}
        return CommandType._validate(val, allowed_values, "Invalid enum value by output dir name")

    @classmethod
    def _validate(cls, val, allowed_values, err_message_prefix):
        if val in allowed_values:
            return allowed_values[val]
        else:
            raise ValueError("{}: {}".format(err_message_prefix, val))


class HtmlHelper:
    @staticmethod
    def generate_summary_str(tables, summary_str: str):
        printable_summary_str: str = summary_str
        for table in tables:
            printable_summary_str += str(table)
            printable_summary_str += "\n\n"
        return printable_summary_str

    @staticmethod
    def generate_summary_html(html_tables, summary_str: str) -> str:
        table_tuples = [(h.header, h.table) for h in html_tables]

        html_sep = HtmlGenerator.generate_separator(tag="hr", breaks=2)
        return (
            HtmlGenerator()
            .append_paragraphs(summary_str.splitlines())
            .begin_html_tag()
            .add_basic_table_style()
            .append_html_tables(
                table_tuples, separator=html_sep, header_type="h1", additional_separator_at_beginning=True
            )
            .render()
        )

    @staticmethod
    def _add_summary_as_html_paragraphs(soup, summary_str):
        lines = summary_str.splitlines()
        for line in lines:
            p = soup.new_tag("p")
            p.append(line)
            soup.append(p)
