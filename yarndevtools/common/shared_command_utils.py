from enum import Enum
from os.path import expanduser

from pythoncommons.email import EmailAccount, EmailConfig
from pythoncommons.file_utils import FileUtils
from pythoncommons.git_constants import ORIGIN
from pythoncommons.html_utils import HtmlGenerator

from yarndevtools.constants import LATEST_DATA_ZIP_LINK_NAME

SECRET_PROJECTS_DIR = FileUtils.join_path(expanduser("~"), ".secret", "projects", "cloudera")


class EnvVar(Enum):
    IGNORE_SMTP_AUTH_ERROR = "IGNORE_SMTP_AUTH_ERROR"


class YarnDevToolsTestEnvVar(Enum):
    FORCE_COLLECTING_ARTIFACTS = "FORCE_COLLECTING_ARTIFACTS"


class YarnDevToolsEnvVar(Enum):
    PROJECT_DETERMINATION_STRATEGY = "PROJECT_DETERMINATION_STRATEGY"


class RepoType(Enum):
    DOWNSTREAM = "downstream"
    UPSTREAM = "upstream"


class SharedCommandUtils:
    @staticmethod
    def ensure_remote_specified(branch):
        if ORIGIN not in branch:
            return f"{ORIGIN}/{branch}"
        return branch


class FullEmailConfig:
    def __init__(self, args, attachment_file: str = None):
        if attachment_file:
            FileUtils.ensure_file_exists_and_readable(attachment_file)
            self.attachment_file = attachment_file
        self.email_account: EmailAccount = EmailAccount(args.account_user, args.account_password)
        self.email_conf: EmailConfig = EmailConfig(args.smtp_server, args.smtp_port, self.email_account)
        self.sender: str = args.sender
        self.recipients = args.recipients
        self.subject: str = args.subject if "subject" in args else None
        self.attachment_filename: str = args.attachment_filename if "attachment_filename" in args else None

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
    SAVE_PATCH = ("save_patch", False)
    CREATE_REVIEW_BRANCH = ("create_review_branch", False)
    BACKPORT_C6 = ("backport_c6", False)
    UPSTREAM_PR_FETCH = ("upstream_pr_fetch", False)
    SAVE_DIFF_AS_PATCHES = ("save_diff_as_patches", False)
    DIFF_PATCHES_OF_JIRA = ("diff_patches_of_jira", False)
    FETCH_JIRA_UMBRELLA_DATA = ("fetch_jira_umbrella_data", True, "latest-session-upstream-umbrella-fetcher")
    BRANCH_COMPARATOR = ("branch_comparator", True, "latest-session-branchcomparator")
    ZIP_LATEST_COMMAND_DATA = ("zip_latest_command_data", False)
    SEND_LATEST_COMMAND_DATA = ("send_latest_command_data", False)
    JENKINS_TEST_REPORTER = ("jenkins_test_reporter", False)
    UNIT_TEST_RESULT_AGGREGATOR = ("unit_test_result_aggregator", True, "latest-session-unit-test-result-aggregator")
    REVIEW_SHEET_BACKPORT_UPDATER = (
        "review_sheet_backport_updater",
        True,
        "latest-session-review-sheet-backport-updater",
    )

    def __init__(self, value, session_based: bool = False, session_link_name: str = ""):
        self.real_name = value
        self.session_based = session_based

        if session_link_name:
            self.session_link_name = session_link_name
        else:
            self.session_link_name = f"latest-session-{value}"

        self.log_link_name = f"latest-log-{value}"
        self.command_data_name = f"latest-command-data-{value}"
        self.command_data_zip_name: str = f"{LATEST_DATA_ZIP_LINK_NAME}-{value}"

    @staticmethod
    def from_str(val):
        val_to_enum = {ct.name: ct for ct in CommandType}
        if val in val_to_enum:
            return val_to_enum[val]
        else:
            raise NotImplementedError


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
