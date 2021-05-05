from enum import Enum
from os.path import expanduser

from pythoncommons.email import EmailAccount, EmailConfig
from pythoncommons.file_utils import FileUtils

SECRET_DIR = FileUtils.join_path(expanduser("~"), ".secret", "hadoop-reviewsync-snemeth-cloudera")
TOKEN_PICKLE_DIR = FileUtils.join_path(SECRET_DIR, "tokenpickles")


class EnvVar(Enum):
    IGNORE_SMTP_AUTH_ERROR = "IGNORE_SMTP_AUTH_ERROR"


class RepoType(Enum):
    DOWNSTREAM = "downstream"
    UPSTREAM = "upstream"


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
