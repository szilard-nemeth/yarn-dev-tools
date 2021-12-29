import tempfile
import unittest

from tests.test_utilities import Object
from yarndevtools.common.shared_command_utils import FullEmailConfig


class TestFullEmailConfig(unittest.TestCase):
    def test_with_default_args(self):
        args = Object()
        with self.assertRaises(ValueError):
            FullEmailConfig(args)

    def test_only_with_account_user(self):
        args = Object()
        args.account_user = "someUser"
        with self.assertRaises(ValueError) as ve:
            FullEmailConfig(args)
        exc_msg = ve.exception.args[0]
        self.assertIn("account password", exc_msg)

    def test_with_email_account_data_without_smtp_server(self):
        args = Object()
        args.account_user = "someUser"
        args.account_password = "somePassword"
        with self.assertRaises(ValueError):
            FullEmailConfig(args)

    def test_with_email_account_data_without_smtp_port(self):
        args = Object()
        args.account_user = "someUser"
        args.account_password = "somePassword"
        args.smtp_server = "smtpServer"
        with self.assertRaises(ValueError) as ve:
            FullEmailConfig(args)
        exc_msg = ve.exception.args[0]
        self.assertIn("Email SMTP port", exc_msg)

    def test_with_email_account_data_all_specified_without_sender(self):
        args = Object()
        args.account_user = "someUser"
        args.account_password = "somePassword"
        args.smtp_server = "smtpServer"
        args.smtp_port = "smtpPort"
        with self.assertRaises(ValueError):
            FullEmailConfig(args)

    def test_with_email_account_data_all_specified_without_recipients(self):
        args = Object()
        args.account_user = "someUser"
        args.account_password = "somePassword"
        args.smtp_server = "smtpServer"
        args.smtp_port = "smtpPort"
        args.sender = "sender"
        with self.assertRaises(ValueError):
            FullEmailConfig(args)

    def test_with_all_data_specified_but_recipients_is_not_a_list(self):
        args = Object()
        args.account_user = "someUser"
        args.account_password = "somePassword"
        args.smtp_server = "smtpServer"
        args.smtp_port = "smtpPort"
        args.sender = "sender"
        args.subject = "subject"
        args.recipients = "recipients"
        with self.assertRaises(ValueError) as ve:
            FullEmailConfig(args)
        exc_msg = ve.exception.args[0]
        self.assertIn("Email recipients should be a List[str]!", exc_msg)

    def test_with_all_mandatory_data_specified_correctly(self):
        args = Object()
        args.account_user = "someUser"
        args.account_password = "somePassword"
        args.smtp_server = "smtpServer"
        args.smtp_port = "smtpPort"
        args.sender = "sender"
        args.subject = "subject"
        args.recipients = ["recipient1", "recipient2"]

        config = FullEmailConfig(args)
        self.assertIsNone(config.attachment_file)
        self.assertIsNone(config.attachment_filename)
        self.assertIsNotNone(config.email_account)
        self.assertIsNotNone(config.email_conf)
        self.assertEqual("sender", config.sender)
        self.assertEqual(["recipient1", "recipient2"], config.recipients)
        self.assertEqual("subject", config.subject)

    def test_with_all_mandatory_plus_other_data_specified_correctly(self):
        args = Object()
        args.account_user = "someUser"
        args.account_password = "somePassword"
        args.smtp_server = "smtpServer"
        args.smtp_port = "smtpPort"
        args.sender = "sender"
        args.subject = "subject"
        args.recipients = ["recipient1", "recipient2"]
        args.attachment_filename = "attachmentFilename"

        with tempfile.NamedTemporaryFile() as tmp_file:
            config = FullEmailConfig(args, attachment_file=tmp_file.name)
            self.assertEqual(tmp_file.name, config.attachment_file)
            self.assertEqual("attachmentFilename", config.attachment_filename)
            self.assertIsNotNone(config.email_account)
            self.assertIsNotNone(config.email_conf)
            self.assertEqual("sender", config.sender)
            self.assertEqual(["recipient1", "recipient2"], config.recipients)
            self.assertEqual("subject", config.subject)
