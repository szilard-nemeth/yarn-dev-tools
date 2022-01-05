import json
import logging
import os
from dataclasses import dataclass, field, fields
from typing import List, Dict

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
    CdswEnvVar,
)
from yarndevtools.common.shared_command_utils import CommandType

LOG = logging.getLogger(__name__)


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class CdswJobConfig:
    job_name: str
    command_type: CommandType = field(metadata=config(encoder=CommandType, decoder=CommandType.from_str, mm_field=None))
    mandatory_env_vars: List[str] = field(default_factory=list)
    optional_env_vars: List[str] = field(default_factory=list)
    map_env_vars_to_yarn_dev_tools_argument: Dict[str, str] = field(default_factory=dict)
    yarn_dev_tools_arguments: List[str] = field(default_factory=list)


@auto_str
class CdswJobConfigReader:
    ARG_PLACEHOLDER = "$$"

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
        enum_type = self.command_to_env_var_class[self.config.command_type]
        self.valid_env_vars = [e.value for e in enum_type] + [e.value for e in CdswEnvVar]
        self._validate_mandatory_env_var_names()
        self._validate_optional_env_var_names()
        self._ensure_if_mandatory_env_vars_are_set()
        self._ensure_that_mapped_env_vars_are_mandatory()
        self._check_yarn_dev_tools_arguments()

    def _validate_optional_env_var_names(self):
        for env_var_name in self.config.optional_env_vars:
            if env_var_name not in self.valid_env_vars:
                raise ValueError(
                    "Invalid optional env var specified as '{}'. Valid env vars for Command '{}' are: {}".format(
                        env_var_name, self.config.command_type, self.valid_env_vars
                    )
                )

    def _validate_mandatory_env_var_names(self):
        for env_var_name in self.config.mandatory_env_vars:
            if env_var_name not in self.valid_env_vars:
                raise ValueError(
                    "Invalid mandatory env var specified as '{}'. Valid env vars for Command '{}' are: {}".format(
                        env_var_name, self.config.command_type, self.valid_env_vars
                    )
                )

    def _ensure_if_mandatory_env_vars_are_set(self):
        not_found_vars = []
        for env_var in self.config.mandatory_env_vars:
            if env_var not in os.environ:
                not_found_vars.append(env_var)

        if not_found_vars:
            raise ValueError("The following env vars are mandatory but they are not set: {}".format(not_found_vars))

    def _ensure_that_mapped_env_vars_are_mandatory(self):
        not_found_vars = []
        for env_var in self.config.map_env_vars_to_yarn_dev_tools_argument.values():
            if env_var not in os.environ:
                not_found_vars.append(env_var)
        if not_found_vars:
            raise ValueError(
                "The following env vars are optional and they are mapped to YARN dev tools arguments, "
                "so they became mandatory but they are not set: {}".format(not_found_vars)
            )

    def _check_yarn_dev_tools_arguments(self):
        if not self.config.yarn_dev_tools_arguments:
            raise ValueError("Empty YARN dev tools arguments!")

        mapped_vars = self.config.map_env_vars_to_yarn_dev_tools_argument.keys()

        not_found_var_mappings = []
        args_mapped_but_without_placeholders = []
        for arg in self.config.yarn_dev_tools_arguments:
            if self.ARG_PLACEHOLDER in arg:
                split = arg.split(" ")
                if len(split) != 2:
                    raise ValueError(
                        "Expected a mapped argument in format: "
                        "<yarndevtools argument name><SPACE><PLACEHOLDER>. "
                        "For example, '--gsheet-client-secret $$'"
                    )
                arg_name = split[0]
                if arg_name not in mapped_vars:
                    not_found_var_mappings.append(arg_name)
            else:
                # Argument without placeholder
                if arg in mapped_vars:
                    args_mapped_but_without_placeholders.append(arg)
        if not_found_var_mappings:
            raise ValueError("The following yarndevtools arguments are unmapped: {}".format(not_found_var_mappings))

        if args_mapped_but_without_placeholders:
            raise ValueError(
                "The following yarndevtools arguments are not having placeholders but they are mapped: {}".format(
                    args_mapped_but_without_placeholders
                )
            )

    def __repr__(self):
        return self.__str__()
