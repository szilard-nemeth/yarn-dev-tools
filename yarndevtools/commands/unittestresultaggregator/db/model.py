import datetime
from typing import List

from googleapiwrapper.gmail_domain import GmailMessage
from marshmallow import fields, Schema, EXCLUDE

from yarndevtools.commands.unittestresultaggregator.common.model import EmailContentProcessor, EmailMetaData
from yarndevtools.commands.unittestresultaggregator.email.common import EmailContentAggregationResults
from yarndevtools.common.common_model import JobBuildData, MONGO_COLLECTION_JENKINS_REPORTS, JobBuildDataSchema
from yarndevtools.common.db import MongoDbConfig, Database, DBSerializable

MONGO_COLLECTION_EMAIL_CONTENT = "email_data"


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
    def from_message(email_meta: EmailMetaData, lines: List[str]):
        return EmailContent(email_meta.message_id, email_meta.thread_id, email_meta.date, email_meta.subject, lines)

    def serialize(self):
        schema = EmailContentSchema()
        output = schema.dump(self)
        return output


class UTResultAggregatorDatabase(Database):
    def __init__(self, conf: MongoDbConfig):
        super().__init__(conf)
        self._email_content_schema = EmailContentSchema()
        self._build_data_schema = JobBuildDataSchema()

    def find_email_content(self, id: str):
        return super().find_by_id(id, collection_name=MONGO_COLLECTION_EMAIL_CONTENT)

    def find_and_validate_email_content(self, id: str):
        doc = self.find_email_content(id)
        dic = self._email_content_schema.load(doc, unknown=EXCLUDE)
        return EmailContent(**dic)

    def find_and_validate_all_email_content(self):
        result = []
        docs = self.find_all_email_content()
        for doc in docs:
            dic = self._email_content_schema.load(doc, unknown=EXCLUDE)
            result.append(EmailContent(**dic))
        return result

    def find_all_email_content(self):
        return super().find_all(collection_name=MONGO_COLLECTION_EMAIL_CONTENT)

    def save_email_content(self, email_content: EmailContent):
        return super().save(email_content, collection_name=MONGO_COLLECTION_EMAIL_CONTENT, id_field_name="msg_id")

    def find_all_build_data(self):
        return super().find_all(collection_name=MONGO_COLLECTION_JENKINS_REPORTS)

    def find_and_validate_all_build_data(self):
        result = []
        docs = self.find_all_build_data()
        for doc in docs:
            dic = self._build_data_schema.load(doc, unknown=EXCLUDE)
            result.append(JobBuildData.deserialize(dic))
        return result


class DBWriterEmailContentProcessor(EmailContentProcessor):
    def __init__(self, db: UTResultAggregatorDatabase):
        self._db = db

    def process(self, email_meta: EmailMetaData, lines: List[str]):
        email_content = self._db.find_and_validate_email_content(email_meta.message_id)
        # TODO yarndevtoolsv2 DB: Save email meta to DB // store dates of emails as well to mongodb: Write start date, end date, missing dates between start and end date
        #  builds_with_dates = self._aggregation_results._failed_builds.get_dates()
        if email_content:
            merged_lines: List[str] = DBWriterEmailContentProcessor._merge_lists(
                email_content.lines, lines, return_result_if_first_modified=True
            )
            if merged_lines:
                email_content.lines = merged_lines
                self._db.save_email_content(email_content)
        else:
            self._db.save_email_content(EmailContent.from_message(email_meta, lines))

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


class JenkinsJobBuildDataAndEmailContentJoiner:
    # TODO yarndevtoolsv2 DB: This class should aggregate email content data (collection: email_data) with Jenkins reports (collection: reports)
    def __init__(self, db: UTResultAggregatorDatabase):
        self._db = db

    def join(self, result: EmailContentAggregationResults):
        # TODO yarndevtoolsv2 DB: Invoke aggregation logic here
        # build_data = self._db.find_and_validate_all_build_data()
        # email_content = self._db.find_and_validate_all_email_content()
        self._db.find_and_validate_all_build_data()
        self._db.find_and_validate_all_email_content()
        # TODO yarndevtoolsv2 DB: Group failures by job name and build dates --> Verify EmailContent's date?
        # result.start_new_context()
        # result.match_line(line, message.subject)
        # result.finish_context(message, email_meta)
        # result.finish_processing_all()
        print("test")
