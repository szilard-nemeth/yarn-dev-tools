import logging
import smtplib
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


# TODO Remove
class EmailServiceLegacy:
    def __init__(self, email_recipients):
        self.mail_recipient_addresses = email_recipients
        self.email_smtp_server = "smtp.gmail.com"
        self.emal_smtp_port = 465
        self.mail_user = ""  # TODO
        self.mail_password = ""  # TODO

    def send_mail(self, subject, msg):
        if not self.mail_recipient_addresses:
            logger.warning("Cannot send email notification as recipient email addresses are not set!")
            return

        email_from = "YARN jenkins test reporter"
        mail_recipients = ", ".join(self.mail_recipient_addresses)
        email_subject = "%s: %s" % ("YARN Daily unit test report", subject)

        email_msg = MIMEText(str(msg))
        email_msg["From"] = email_from
        email_msg["To"] = mail_recipients
        email_msg["Subject"] = email_subject
        server = smtplib.SMTP_SSL(self.email_smtp_server, self.emal_smtp_port)
        logger.info("Sending mail to recipients: %s", mail_recipients)
        server.ehlo()
        server.login(self.mail_user, self.mail_password)
        server.sendmail(email_from, self.mail_recipient_addresses, email_msg.as_string())
        server.quit()
