import logging
from typing import List, Set

from pythoncommons.email import EmailService, EmailMimeType

from yarndevtools.commands.unittestresultfetcher.db import MailSentTracker, MailSendDataForJob
from yarndevtools.common.common_model import JobBuildData
from yarndevtools.common.shared_command_utils import FullEmailConfig

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
        self._mail_sent_tracker: MailSentTracker = None

    def send_mail(self, build_data: JobBuildData, recipients: Set[str]):
        # TODO Add MailSendProgress class to track how many emails were sent in current session
        LOG.info("Sending report in email for job: %s", build_data.build_url)
        self.email_service.send_mail(
            sender=self.config.full_email_conf.sender,
            subject=self._get_email_subject(build_data),
            body=str(build_data),
            recipients=list(recipients),
            body_mimetype=EmailMimeType.PLAIN,
        )
        LOG.info("Finished sending report in email for job: %s", build_data.build_url)

    @property
    def recipients(self):
        return self.config.full_email_conf.recipients

    @staticmethod
    def _get_email_subject(build_data: JobBuildData):
        if build_data.is_valid:
            email_subject = f"{EMAIL_SUBJECT_PREFIX} Failed tests with build: {build_data.build_url}"
        else:
            email_subject = f"{EMAIL_SUBJECT_PREFIX} Error, build is invalid: {build_data.build_url}"
        return email_subject

    def process(self, build_data) -> MailSendDataForJob or None:
        self._verify_tracker_state()
        if self.config.send_mail:
            send_to_recipients = self._mail_sent_tracker.is_mail_sent(build_data.build_url, self.recipients)
            if send_to_recipients or self.config.force_send_email:
                self.send_mail(build_data, send_to_recipients)
                self.mark_sent(build_data.build_url, build_data.job_name, send_to_recipients)
            else:
                LOG.info(
                    "Not sending report of job URL %s, as it was already sent before "
                    "for all of these recipients: %s",
                    build_data.build_url,
                    self.recipients,
                )
            return self._get_send_state(build_data)
        return None

    def mark_sent(self, build_url, job_name, recipients):
        self._verify_tracker_state()
        self._mail_sent_tracker.mark_sent(build_url, job_name, recipients)

    def load_send_state_from_db(self, db):
        mail_send_data_for_all_jobs: List[MailSendDataForJob] = db.load_email_sent_state()
        self._mail_sent_tracker = MailSentTracker(mail_send_data_for_all_jobs)

    def _get_send_state(self, build_data) -> MailSendDataForJob:
        self._verify_tracker_state()
        return self._mail_sent_tracker.get_val(build_data.build_url, build_data.job_name)

    def _verify_tracker_state(self):
        if self._mail_sent_tracker is None:
            raise ValueError("Should call 'load_send_state_from_db' method first!")
