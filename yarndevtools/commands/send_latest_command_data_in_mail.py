import logging
import os

from pythoncommons.email import EmailService
from pythoncommons.file_utils import FileUtils
from pythoncommons.zip_utils import ZipFileUtils

from yarndevtools.common.shared_command_utils import FullEmailConfig
from yarndevtools.constants import SUMMARY_FILE_HTML

LOG = logging.getLogger(__name__)


class SendLatestCommandDataInEmailConfig:
    def __init__(self, args, attachment_file):
        self.email: FullEmailConfig = FullEmailConfig(args, attachment_file)
        self.email_body_file: str = args.email_body_file


class SendLatestCommandDataInEmail:
    def __init__(self, args, attachment_file: str):
        self.config = SendLatestCommandDataInEmailConfig(args, attachment_file)

    def run(self):
        LOG.info("Starting sending latest command data in email.\n" f"Config: {str(self.config)}")

        zip_extract_dest = FileUtils.join_path(os.sep, "tmp", "extracted_zip")
        ZipFileUtils.extract_zip_file(self.config.email.attachment_file, zip_extract_dest)

        # Pick file from zip that will be the email's body
        email_body_file = FileUtils.join_path(os.sep, zip_extract_dest, self.config.email_body_file)
        FileUtils.ensure_file_exists(email_body_file)
        email_body_contents: str = FileUtils.read_file(email_body_file)

        email_service = EmailService(self.config.email.email_conf)
        email_service.send_mail(
            self.config.email.sender,
            self.config.email.subject,
            email_body_contents,
            self.config.email.recipients,
            self.config.email.attachment_file,
            body_mimetype="html",
            override_attachment_filename=self.config.email.attachment_filename,
        )
        LOG.info("Finished sending email to recipients")
