import logging
import os
from smtplib import SMTPAuthenticationError

from pythoncommons.email import EmailService, EmailMimeType
from pythoncommons.file_utils import FileUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.zip_utils import ZipFileUtils

from yarndevtools.common.shared_command_utils import FullEmailConfig, EnvVar

LOG = logging.getLogger(__name__)


class SendLatestCommandDataInEmailConfig:
    def __init__(self, args, attachment_file):
        self.email: FullEmailConfig = FullEmailConfig(args, attachment_file)
        self.email_body_file: str = args.email_body_file

    def __str__(self):
        return f"Email config: {self.email}\n" f"Email body file: {self.email_body_file}\n"


class SendLatestCommandDataInEmail:
    def __init__(self, args, attachment_file: str):
        self.config = SendLatestCommandDataInEmailConfig(args, attachment_file)

    def run(self):
        LOG.info(f"Starting sending latest command data in email.\n Config: {str(self.config)}")

        zip_extract_dest = FileUtils.join_path(os.sep, "tmp", "extracted_zip")
        ZipFileUtils.extract_zip_file(self.config.email.attachment_file, zip_extract_dest)

        # Pick file from zip that will be the email's body
        email_body_file = FileUtils.join_path(os.sep, zip_extract_dest, self.config.email_body_file)
        FileUtils.ensure_file_exists(email_body_file)
        email_body_contents: str = FileUtils.read_file(email_body_file)

        body_mimetype: EmailMimeType = self._determine_body_mimetype_by_attachment(email_body_file)
        email_service = EmailService(self.config.email.email_conf)
        try:
            email_service.send_mail(
                self.config.email.sender,
                self.config.email.subject,
                email_body_contents,
                self.config.email.recipients,
                self.config.email.attachment_file,
                body_mimetype=body_mimetype,
                override_attachment_filename=self.config.email.attachment_filename,
            )
        except SMTPAuthenticationError as smtpe:
            ignore_smpth_auth_env: str = OsUtils.get_env_value(EnvVar.IGNORE_SMTP_AUTH_ERROR.value, "")
            LOG.info(f"Recognized env var '{EnvVar.IGNORE_SMTP_AUTH_ERROR.value}': {ignore_smpth_auth_env}")
            if not ignore_smpth_auth_env:
                raise smtpe
            else:
                # Swallow exeption
                LOG.exception(
                    f"SMTP auth error occurred but env var " f"'{EnvVar.IGNORE_SMTP_AUTH_ERROR.value}' was set",
                    exc_info=True,
                )
        LOG.info("Finished sending email to recipients")

    @staticmethod
    def _determine_body_mimetype_by_attachment(email_body_file: str) -> EmailMimeType:
        if email_body_file.endswith(".html"):
            return EmailMimeType.HTML
        else:
            return EmailMimeType.PLAIN
