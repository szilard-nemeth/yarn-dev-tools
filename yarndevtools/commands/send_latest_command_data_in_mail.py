import logging
import os

from pythoncommons.email import EmailConfig, EmailAccount, EmailService
from pythoncommons.file_utils import FileUtils
from pythoncommons.zip_utils import ZipFileUtils

from yarndevtools.constants import SUMMARY_FILE_HTML

LOG = logging.getLogger(__name__)


class Config:
    def __init__(self, args, attachment_file: str):
        FileUtils.ensure_file_exists_and_readable(attachment_file)
        self.attachment_file = attachment_file
        self.email_account = EmailAccount(args.account_user, args.account_password)
        self.email_conf = EmailConfig(args.smtp_server, args.smtp_port, self.email_account)
        self.sender = args.sender
        self.recipients = args.recipients
        self.subject = args.subject
        self.attachment_filename = args.attachment_filename

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


class SendLatestCommandDataInEmail:
    def __init__(self, args, attachment_file: str):
        self.config = Config(args, attachment_file)

    def run(self):
        LOG.info("Starting sending latest command data in email. Details: \n" f"{str(self.config)}")

        zip_extract_dest = FileUtils.join_path(os.sep, "tmp", "extracted_zip")
        ZipFileUtils.extract_zip_file(self.config.attachment_file, zip_extract_dest)

        summary_html = FileUtils.join_path(os.sep, zip_extract_dest, SUMMARY_FILE_HTML)
        FileUtils.ensure_file_exists(summary_html)
        email_body = FileUtils.read_file(summary_html)

        email_service = EmailService(self.config.email_conf)
        email_service.send_mail(
            self.config.sender,
            self.config.subject,
            email_body,
            self.config.recipients,
            self.config.attachment_file,
            body_mimetype="html",
            override_attachment_filename=self.config.attachment_filename,
        )
        LOG.info("Finished sending email to recipients")
