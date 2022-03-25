import logging
import os
from smtplib import SMTPAuthenticationError
from typing import Callable

from pythoncommons.email import EmailService, EmailMimeType
from pythoncommons.file_utils import FileUtils
from pythoncommons.os_utils import OsUtils
from pythoncommons.zip_utils import ZipFileUtils

from yarndevtools.commands_common import CommandAbs, EmailArguments
from yarndevtools.common.shared_command_utils import FullEmailConfig, EnvVar, CommandType
from yarndevtools.constants import SummaryFile, LATEST_DATA_ZIP_LINK_NAME
from yarndevtools.yarn_dev_tools_config import YarnDevToolsConfig

LOG = logging.getLogger(__name__)


class SendLatestCommandDataInEmailConfig:
    def __init__(self, args, attachment_file):
        self.email: FullEmailConfig = FullEmailConfig(args, attachment_file)
        self.email_body_file: str = args.email_body_file
        self.prepend_email_body_with_text: str = args.prepend_email_body_with_text
        self.send_attachment: bool = args.send_attachment

    def __str__(self):
        return (
            f"Email config: {self.email}\n"
            f"Email body file: {self.email_body_file}\n"
            f"Send attachment: {self.send_attachment}\n"
        )


class SendLatestCommandDataInEmail(CommandAbs):
    def __init__(self, args, attachment_file: str):
        self.config = SendLatestCommandDataInEmailConfig(args, attachment_file)

    @staticmethod
    def create_parser(subparsers, func_to_call: Callable):
        parser = subparsers.add_parser(
            CommandType.SEND_LATEST_COMMAND_DATA.name,
            help="Sends latest command data in email." "Example: --dest_dir /tmp",
        )
        parser.add_argument(
            "--file-as-email-body-from-zip",
            dest="email_body_file",
            required=False,
            type=str,
            help="The specified file from the latest command data zip will be added to the email body.",
            default=SummaryFile.HTML.value,
        )

        parser.add_argument(
            "--prepend_email_body_with_text",
            dest="prepend_email_body_with_text",
            required=False,
            type=str,
            help="Prepend the specified text to the email's body.",
            default=SummaryFile.HTML.value,
        )

        parser.add_argument(
            "-s",
            "--send-attachment",
            dest="send_attachment",
            action="store_true",
            default=False,
            help="Send command data as email attachment",
        )
        EmailArguments.add_email_arguments(parser)
        parser.set_defaults(func=func_to_call)

    @staticmethod
    def execute(args, parser=None):
        file_to_send = FileUtils.join_path(YarnDevToolsConfig.PROJECT_OUT_ROOT, LATEST_DATA_ZIP_LINK_NAME)
        send_latest_cmd_data = SendLatestCommandDataInEmail(args, file_to_send)
        send_latest_cmd_data.run()

    def run(self):
        LOG.info(f"Starting sending latest command data in email.\n Config: {str(self.config)}")

        zip_extract_dest = FileUtils.join_path(os.sep, "tmp", "extracted_zip")
        ZipFileUtils.extract_zip_file(self.config.email.attachment_file, zip_extract_dest)

        # Pick file from zip that will be the email's body
        email_body_file = FileUtils.join_path(os.sep, zip_extract_dest, self.config.email_body_file)
        FileUtils.ensure_file_exists(email_body_file)
        email_body_contents: str = FileUtils.read_file(email_body_file)

        if self.config.prepend_email_body_with_text:
            LOG.debug("Prepending email body with: %s", self.config.prepend_email_body_with_text)
            email_body_contents = self.config.prepend_email_body_with_text + email_body_contents

        body_mimetype: EmailMimeType = self._determine_body_mimetype_by_attachment(email_body_file)
        email_service = EmailService(self.config.email.email_conf)
        kwargs = {
            "body_mimetype": body_mimetype,
        }

        if self.config.send_attachment:
            kwargs["attachment_file"] = self.config.email.attachment_file
            kwargs["override_attachment_filename"] = self.config.email.attachment_filename

        try:
            email_service.send_mail(
                self.config.email.sender,
                self.config.email.subject,
                email_body_contents,
                self.config.email.recipients,
                **kwargs,
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
