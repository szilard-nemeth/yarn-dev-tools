import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import List, Dict, Any

from dataclasses_json import dataclass_json, LetterCase, config
from pythoncommons.date_utils import DateUtils
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
class EmailSettings:
    enabled: bool
    send_attachment: bool
    attachment_filename: str
    subject: str


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class CdswJobConfig:
    job_name: str
    command_type: CommandType = field(metadata=config(encoder=CommandType, decoder=CommandType.from_str, mm_field=None))
    mandatory_env_vars: List[str] = field(default_factory=list)
    optional_env_vars: List[str] = field(default_factory=list)
    map_env_vars_to_yarn_dev_tools_argument: Dict[str, str] = field(default_factory=dict)
    yarn_dev_tools_arguments: List[str] = field(default_factory=list)
    variables: Dict[str, str] = field(default_factory=dict)
    resolved_variables: Dict[str, str] = field(default_factory=dict)
    email_settings: EmailSettings = None


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
        self.variables = Variables(self.config.variables)
        self.config.resolved_variables = self.variables.resolved_vars
        self.substitute_variables()

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

    def substitute_variables(self):
        variable_subst_fields = {"email_settings.subject", "email_settings.attachment_filename"}
        ph = Variables.VAR_PLACEHOLDER
        for field_spec in variable_subst_fields:
            orig_value = self._find_config_attribute_by_field_spec(self.config, field_spec)
            if not isinstance(orig_value, str):
                raise ValueError(
                    "Unexpected configuration attribute '{}', object: {}. Expected type of str!".format(
                        field_spec, orig_value
                    )
                )
            vars_to_replace = Variables.find_vars_to_replace(orig_value)

            # We don't need recursive replace here
            mod_value = orig_value
            for var in vars_to_replace:
                if var not in self.config.resolved_variables:
                    raise ValueError("Variable '{}' is not defined!".format(var))
                resolved_var = self.config.resolved_variables[var]
                mod_value = mod_value.replace(f"{ph}{var}{ph}", f"{resolved_var}")
            self._set_config_attribute_by_field_spec(self.config, field_spec, mod_value)

    @staticmethod
    def _find_config_attribute_by_field_spec(obj, field_spec):
        fields = field_spec.split(".")
        for i, attr in enumerate(fields):
            if not hasattr(obj, attr):
                raise ValueError("Config object has no field with field spec '{}'!", field_spec)
            obj = getattr(obj, attr)
        return obj

    @staticmethod
    def _set_config_attribute_by_field_spec(obj, field_spec, value: Any):
        fields = field_spec.split(".")
        parent = None
        for i, attr in enumerate(fields):
            if not hasattr(obj, attr):
                raise ValueError("Config object has no field with field spec '{}'!", field_spec)
            parent = obj
            obj = getattr(obj, attr)
        LOG.debug("Setting attribute of object '%s.%s' to value '%s'", parent, attr, value)
        setattr(parent, attr, value)

    def __repr__(self):
        return self.__str__()


class Variables:
    VAR_PLACEHOLDER = "$$"
    BUILT_IN_VARIABLES = {"JOB_START_DATE": DateUtils.get_current_datetime()}

    def __init__(self, orig_vars: Dict[str, str]):
        self.orig_vars = orig_vars
        self.resolved_vars: Dict[str, str] = orig_vars.copy()
        self.check_variables()

    def check_variables(self):
        for var_name, raw_var in self.orig_vars.items():
            if self.VAR_PLACEHOLDER in raw_var:
                vars_to_replace = self.find_vars_to_replace(raw_var)
                modified_var = self._replace_vars(self.resolved_vars, raw_var, vars_to_replace)
                self.resolved_vars[var_name] = modified_var

    @staticmethod
    def find_vars_to_replace(raw_var):
        ph = Variables.VAR_PLACEHOLDER
        var_sep = re.escape(ph)
        indices = [m.span() for m in re.finditer(f"{var_sep}.*?{var_sep}", raw_var)]
        found_placeholders = re.findall(f"{var_sep}", raw_var)
        if len(found_placeholders) % 2 != 0:
            raise ValueError("Malformed variable declaration in variable string: {}".format(raw_var))
        vars_to_replace = [raw_var[i + len(ph) : j - len(ph)] for i, j in indices]

        if "" in vars_to_replace:
            raise ValueError("Found malformed (empty) variable declaration in raw env var: {}".format(raw_var))
        return vars_to_replace

    @staticmethod
    def _replace_vars(vars, raw_var, vars_to_replace: List[str]):
        ph = Variables.VAR_PLACEHOLDER
        builtin = Variables.BUILT_IN_VARIABLES
        for var in vars_to_replace:
            if var in builtin:
                # Built-in variable
                if var in vars:
                    raise ValueError(
                        "Cannot use variables with the same name as built-in variables. "
                        "Built-ins: {}".format(builtin)
                    )
                resolved_var = builtin[var]
                raw_var = raw_var.replace(f"{ph}{var}{ph}", f"{resolved_var}")
                vars[var] = raw_var
            elif var not in vars:
                # variable does not exist
                raise ValueError("Cannot resolve variable '{}' in raw var: {}".format(var, raw_var))
            elif ph in vars[var]:
                # Variable contains PLACEHOLDERs so it's another variable
                vars_to_replace = Variables.find_vars_to_replace(vars[var])
                resolved_var = Variables._replace_vars(vars, vars[var], vars_to_replace)
                vars[var] = resolved_var
                raw_var = raw_var.replace(f"{ph}{var}{ph}", f"{resolved_var}")
            else:
                # Simple variable
                resolved_var = vars[var]
                raw_var = raw_var.replace(f"{ph}{var}{ph}", f"{resolved_var}")
        return raw_var
