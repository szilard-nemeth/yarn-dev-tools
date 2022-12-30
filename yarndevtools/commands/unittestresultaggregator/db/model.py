import datetime

from marshmallow import fields, Schema

from yarndevtools.common.common_model import AggregatorEntity
from yarndevtools.common.db import DBSerializable
import logging

MONGO_COLLECTION_EMAIL_CONTENT = "email_data"
LOG = logging.getLogger(__name__)


class EmailContentSchema(Schema):
    msg_id = fields.Str(required=True)
    thread_id = fields.Str(required=True)
    date = fields.DateTime(required=True)
    subject = fields.Str(required=True)
    build_url = fields.Str(required=True)
    job_name = fields.Str(required=True)
    build_number = fields.Str(required=True)
    lines = fields.List(fields.Str)


class EmailContent(DBSerializable, AggregatorEntity):
    def __init__(self, msg_id, thread_id, date, subject, build_url, job_name, build_number, lines):
        self.msg_id = msg_id
        self.thread_id = thread_id
        self.date: datetime.datetime = date
        self.subject = subject
        self.build_url = build_url
        self.job_name = job_name
        self.build_number = build_number
        self.lines = lines

    def serialize(self):
        schema = EmailContentSchema()
        output = schema.dump(self)
        return output

    def job_name(self) -> str:
        return self.job_name

    def build_number(self) -> str:
        return self.build_number
