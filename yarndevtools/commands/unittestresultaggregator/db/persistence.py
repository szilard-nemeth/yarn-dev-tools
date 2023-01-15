from typing import List

from marshmallow import EXCLUDE

from yarndevtools.commands.unittestresultaggregator.common.model import EmailContentProcessor
from yarndevtools.commands.unittestresultaggregator.db.model import (
    EmailContent,
    EmailContentSchema,
    MONGO_COLLECTION_EMAIL_CONTENT,
)
from yarndevtools.common.common_model import JobBuildDataSchema, MONGO_COLLECTION_JENKINS_BUILD_DATA, JobBuildData
from yarndevtools.common.db import Database, MongoDbConfig


class UTResultAggregatorDatabase(Database):
    def __init__(self, conf: MongoDbConfig):
        super().__init__(conf)
        self._email_content_schema = EmailContentSchema()
        self._build_data_schema = JobBuildDataSchema()

    def find_email_content(self, id: str):
        return super().find_by_id(id, collection_name=MONGO_COLLECTION_EMAIL_CONTENT)

    def find_and_validate_email_content(self, id: str):
        doc = self.find_email_content(id)
        if not doc:
            return None

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
        return super().find_all(collection_name=MONGO_COLLECTION_JENKINS_BUILD_DATA)

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

    def process(self, new_email_content: EmailContent):
        email_content = self._db.find_and_validate_email_content(new_email_content.msg_id)
        if email_content:
            merged_lines: List[str] = DBWriterEmailContentProcessor._merge_lists(
                email_content.lines, new_email_content.lines, return_result_if_first_modified=True
            )
            if merged_lines:
                email_content.lines = merged_lines
                self._db.save_email_content(email_content)
        else:
            self._db.save_email_content(new_email_content)

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
