import datetime
from typing import List

from googleapiwrapper.gmail_domain import GmailMessage
from marshmallow import fields, Schema, EXCLUDE

from yarndevtools.commands.unittestresultaggregator.common.model import EmailContentProcessor
from yarndevtools.common.db import MongoDbConfig, Database, DBSerializable


class EmailContentSchema(Schema):
    msg_id = fields.Str(required=True)
    thread_id = fields.Str(required=True)
    date = fields.DateTime(required=True)
    subject = fields.Str(required=True)
    lines = fields.List(fields.Str)


class EmailContent(DBSerializable):
    msg_id: str
    thread_id: str
    date: datetime.datetime
    subject: str
    lines: List[str]

    def __init__(self, msg_id, thread_id, date, subject, lines):
        self.msg_id = msg_id
        self.thread_id = thread_id
        self.date = date
        self.subject = subject
        self.lines = lines

    @staticmethod
    def from_message(message: GmailMessage, lines: List[str]):
        return EmailContent(message.msg_id, message.thread_id, message.date, message.subject, lines)

    def serialize(self):
        schema = EmailContentSchema()
        output = schema.dump(self)
        return output


class UTResultAggregatorDatabase(Database):
    def __init__(self, conf: MongoDbConfig):
        super().__init__(conf)

    def save_email_content(self, email_content: EmailContent):
        return super().save(email_content, collection_name="email_data", id_field_name="msg_id")


class DBWriterEmailContentProcessor(EmailContentProcessor):
    def __init__(self, db: UTResultAggregatorDatabase):
        self._db = db

    def process(self, message: GmailMessage, lines: List[str]):
        email_content = EmailContent.from_message(message, lines)
        doc = self._db.find_by_id(message.msg_id, collection_name="email_data")
        if doc:
            schema = EmailContentSchema()
            email_content: EmailContent = EmailContent(**schema.load(doc, unknown=EXCLUDE))
            merged_lines: List[str] = DBWriterEmailContentProcessor._merge_lists(
                email_content.lines, lines, return_result_if_first_modified=True
            )
            if merged_lines:
                email_content.lines = merged_lines
                self._db.save_email_content(email_content)
        else:
            self._db.save_email_content(email_content)

    @staticmethod
    def _merge_lists(l1, l2, return_result_if_first_modified=False):
        in_first = set(l1)
        in_second = set(l2)
        in_second_but_not_in_first = in_second - in_first
        result = l1 + list(in_second_but_not_in_first)

        if return_result_if_first_modified:
            if in_second_but_not_in_first:
                return result
            else:
                return None
        return result
