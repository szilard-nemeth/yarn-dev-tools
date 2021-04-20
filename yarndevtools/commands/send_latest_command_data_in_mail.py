import logging
import os

from pythoncommons.email import EmailService
from pythoncommons.file_utils import FileUtils
from pythoncommons.zip_utils import ZipFileUtils

from yarndevtools.common.shared_command_utils import FullEmailConfig
from yarndevtools.constants import SUMMARY_FILE_HTML

LOG = logging.getLogger(__name__)


class SendLatestCommandDataInEmail:
    def __init__(self, args, attachment_file: str):
        self.config: FullEmailConfig = FullEmailConfig(args, attachment_file)

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
