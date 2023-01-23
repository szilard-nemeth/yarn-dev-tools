from typing import List

from pythoncommons.email import EmailService, EmailMimeType

from yarndevtools.commands.unittestresultfetcher.db import JenkinsJobResults
from yarndevtools.common.common_model import JobBuildData
from yarndevtools.common.shared_command_utils import FullEmailConfig
import logging

LOG = logging.getLogger(__name__)

EMAIL_SUBJECT_PREFIX = "YARN Daily unit test report:"


class EmailConfig:
    def __init__(self, args):
        self.full_email_conf: FullEmailConfig = FullEmailConfig(args, allow_empty_subject=True)
        skip_email = args.skip_email if hasattr(args, "skip_email") else False
        self.force_send_email = args.force_send_email if hasattr(args, "force_send_email") else False
        self.send_mail: bool = not skip_email or self.force_send_email
        self.reset_email_sent_state: List[str] = (
            args.reset_sent_state_for_jobs if hasattr(args, "reset_sent_state_for_jobs") else []
        )
        if not self.send_mail:
            LOG.info("Skip sending emails, as per configuration.")

    def validate(self, job_names: List[str]):
        if not all([reset in job_names for reset in self.reset_email_sent_state]):
            raise ValueError(
                "Not all jobs are recognized while trying to reset email sent state for jobs! "
                "Valid job names: {}, Current job names: {}".format(job_names, self.reset_email_sent_state)
            )


class Email:
    def __init__(self, config):
        self.config: EmailConfig = config
        self.email_service = EmailService(config.full_email_conf.email_conf, batch_mode=True)

    def initialize(self, job_results: JenkinsJobResults):
        # Try to reset email sent state of asked jobs
        if self.config.reset_email_sent_state:
            LOG.info("Resetting email sent state to False on these jobs: %s", self.config.reset_email_sent_state)
            for job_name in self.config.reset_email_sent_state:
                # Jenkins job result can be empty at this point if cache was empty for this job or not found
                if job_name in job_results:
                    job_results[job_name].reset_mail_sent_state()

    def send_mail(self, build_data: JobBuildData):
        # TODO Add MailSendProgress class to track how many emails were sent
        LOG.info("Sending report in email for job: %s", build_data.build_url)
        self.email_service.send_mail(
            sender=self.config.full_email_conf.sender,
            subject=self._get_email_subject(build_data),
            body=str(build_data),
            recipients=self.config.full_email_conf.recipients,
            body_mimetype=EmailMimeType.PLAIN,
        )
        LOG.info("Finished sending report in email for job: %s", build_data.build_url)

    @staticmethod
    def _get_email_subject(build_data: JobBuildData):
        if build_data.is_valid:
            email_subject = f"{EMAIL_SUBJECT_PREFIX} Failed tests with build: {build_data.build_url}"
        else:
            email_subject = f"{EMAIL_SUBJECT_PREFIX} Error, build is invalid: {build_data.build_url}"
        return email_subject

    def process(self, build_data, job_result):
        if self.config.send_mail:
            if not build_data.is_mail_sent or self.config.force_send_email:
                self.send_mail(build_data)
                job_result.mark_sent(build_data.build_url)
            else:
                LOG.info(
                    "Not sending report of job URL %s, as it was already sent before on %s.",
                    build_data.build_url,
                    build_data.sent_date,
                )
