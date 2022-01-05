import json
import logging
import os
from dataclasses import dataclass, field, fields
from typing import List

from dataclasses_json import dataclass_json, LetterCase, config
from pythoncommons.file_utils import JsonFileUtils
from pythoncommons.string_utils import auto_str

from yarndevtools.cdsw.common_python.constants import (
    JiraUmbrellaCheckerEnvVar,
    BranchComparatorEnvVar,
    UnitTestResultAggregatorEnvVar,
    UnitTestResultFetcherEnvVar,
    ReviewSheetBackportUpdaterEnvVar,
    ReviewSyncEnvVar,
)
from yarndevtools.common.shared_command_utils import CommandType

LOG = logging.getLogger(__name__)


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class CdswJobConfig:
    job_name: str
    command_type: CommandType = field(metadata=config(encoder=CommandType, decoder=CommandType.from_str, mm_field=None))
    mandatory_env_vars: List[str] = field(default_factory=list)


@auto_str
class CdswJobConfigReader:
    command_to_env_var_class = {
        CommandType.JIRA_UMBRELLA_DATA_FETCHER: JiraUmbrellaCheckerEnvVar,
        CommandType.BRANCH_COMPARATOR: BranchComparatorEnvVar,
        CommandType.UNIT_TEST_RESULT_FETCHER: UnitTestResultFetcherEnvVar,
        CommandType.UNIT_TEST_RESULT_AGGREGATOR: UnitTestResultAggregatorEnvVar,
        CommandType.REVIEW_SHEET_BACKPORT_UPDATER: ReviewSheetBackportUpdaterEnvVar,
        CommandType.REVIEWSYNC: ReviewSyncEnvVar,
    }

    def __init__(self, data):
        self.data = data
        self.config: CdswJobConfig = self._parse()
        self._validate()

    @staticmethod
    def read_from_file(file):
        data_dict = JsonFileUtils.load_data_from_json_file(file)
        return CdswJobConfigReader(data_dict)

    def _parse(self):
        job_config = CdswJobConfig.from_json(json.dumps(self.data))
        LOG.info("Job config: %s", job_config)
        return job_config

    def _validate(self):
        self._validate_mandatory_env_vars()

    def _validate_mandatory_env_vars(self):
        enum_type = self.command_to_env_var_class[self.config.command_type]
        valid_env_vars = [e.value for e in enum_type]
        for env_var_name in self.config.mandatory_env_vars:
            if env_var_name not in valid_env_vars:
                raise ValueError(
                    "Invalid env var specified as '{}'. Valid env vars for Command '{}' are: {}".format(
                        env_var_name, self.config.command_type, valid_env_vars
                    )
                )

    def __repr__(self):
        return self.__str__()
