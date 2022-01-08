import json
import logging
import os
import re
from copy import copy
from dataclasses import dataclass, field
from typing import List, Dict, Any, Callable

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

YARN_DEV_TOOLS_VAR_OVERRIDE_TEMPLATE = "Found argument in yarn_dev_tools_arguments and runconfig.yarn_dev_tools_arguments: '%s'. The latter will take predence."

LOG = logging.getLogger(__name__)


@dataclass
class PlaceHolderMatcher:
    placeholder: str
    format_regex: str
    escaped_placeholder: str = None
    placeholder_len: int = -1

    def __post_init__(self):
        self.escaped_placeholder = re.escape(self.placeholder)
        self.placeholder_len = len(self.placeholder)
        self.format_regex = self.format_regex.replace("<PH>", self.escaped_placeholder)


NORMAL_VAR_MATCHER = PlaceHolderMatcher("$$", "<PH>.*?<PH>")
ENV_VAR_MATCHER_REGEX = "ENV\\((.*?)\\)"


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class CdswRun:
    yarn_dev_tools_arguments: List[str] = field(default_factory=list)
    variables: Dict[str, str] = field(default_factory=dict)

    # Dynamic properties
    resolved_variables: Dict[str, str] = field(default_factory=dict)


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class EmailSettings:
    enabled: bool
    send_attachment: bool
    attachment_file_name: str
    subject: str


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class DriveApiUploadSettings:
    enabled: bool
    file_name: str


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class CdswJobConfig:
    job_name: str
    command_type: CommandType = field(metadata=config(encoder=CommandType, decoder=CommandType.from_str, mm_field=None))
    email_settings: EmailSettings
    drive_api_upload_settings: DriveApiUploadSettings
    runs: List[CdswRun] = field(default_factory=list)
    mandatory_env_vars: List[str] = field(default_factory=list)
    optional_env_vars: List[str] = field(default_factory=list)
    yarn_dev_tools_arguments: List[str] = field(default_factory=list)
    global_variables: Dict[str, str] = field(default_factory=dict)


@dataclass
class FieldSpec:
    MARKER = "[]"
    val: str
    fields: List[str] = field(default_factory=list)

    def __post_init__(self):
        split = self.val.split(".")
        if "" in split:
            raise ValueError("Invalid field spec: {}".format(self.val))
        if split.count(FieldSpec.MARKER) > 1:
            raise ValueError(
                "Invalid field spec: {}. Field specs should only have one list definition with marker '{}'".format(
                    self.val, FieldSpec.MARKER
                )
            )

        self.fields = split


@dataclass
class FieldSpecInstance:
    _based_on: FieldSpec
    _field_spec: FieldSpec = None
    index: int = None

    @staticmethod
    def create_from(field_spec: FieldSpec, index: int = None):
        return FieldSpecInstance(field_spec, index=index)

    def __post_init__(self):
        if self.index and self._based_on.val.count(FieldSpec.MARKER) == 0:
            raise ValueError(
                "Invalid field spec instance: {}. Index should be specified only if there is a list marker '{}'".format(
                    self.val, FieldSpec.MARKER
                )
            )
        self._field_spec = copy(self._based_on)
        if self.index is not None:
            self._field_spec.val = self._replace_index(self._field_spec.val)
            new_fields = []
            for f in self._field_spec.fields:
                new_fields.append(self._replace_index(f))
            self._field_spec.fields = new_fields

    def _replace_index(self, s):
        c1 = FieldSpec.MARKER[0]
        c2 = FieldSpec.MARKER[1]
        return re.sub(r"\[(.*)\]", f"{c1}{self.index}{c2}", s)

    @property
    def fields(self):
        return self._field_spec.fields

    @property
    def val(self):
        return self._field_spec.val


@dataclass
class ResolvedFieldSpec:
    name: str
    value: Any
    parent: Any


@auto_str
class CdswJobConfigReader:
    VARIABLE_SUBSTITUTION_FIELDS = [
        FieldSpec("email_settings.subject"),
        FieldSpec("email_settings.attachment_file_name"),
        FieldSpec("drive_api_upload_settings.file_name"),
        FieldSpec("yarn_dev_tools_arguments"),
        FieldSpec("runs[].yarn_dev_tools_arguments"),
    ]

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
        if not self.config.runs:
            raise ValueError("Section 'runs' must be defined and cannot be empty!")
        if not self.config.yarn_dev_tools_arguments:
            raise ValueError("Empty YARN dev tools arguments!")

        enum_type = self.command_to_env_var_class[self.config.command_type]
        self.field_spec_resolver = FieldSpecResolver(self.config)
        self.field_spec_replacer = FieldSpecReplacer(self.config, self.field_spec_resolver)

        self.environment_variables = EnvironmentVariables(
            self.config.mandatory_env_vars,
            self.config.optional_env_vars,
            self.config.command_type,
            enum_type,
            self.field_spec_replacer,
        )
        self.global_variables = RegularVariables(self.config.global_variables)
        self.config.resolved_variables = self.global_variables.resolved_vars
        self._setup_vars_for_runs()
        self.environment_variables.substitute_env_vars(self.field_spec_resolver, FieldSpec("yarn_dev_tools_arguments"))
        self.environment_variables.substitute_env_vars(
            self.field_spec_resolver, FieldSpec("runs[].yarn_dev_tools_arguments")
        )
        self.field_spec_replacer.substitute_regular_variables_in_fields(
            self.field_spec_resolver, self.VARIABLE_SUBSTITUTION_FIELDS, self._substitute_regular_variable_in_str
        )
        self._finalize_yarn_dev_tools_arguments()

    def _setup_vars_for_runs(self):
        for run in self.config.runs:
            vars = copy(self.global_variables)
            vars.add_more_vars(run.variables)
            run.resolved_variables = vars.resolved_vars

    def _finalize_yarn_dev_tools_arguments(self):
        for run in self.config.runs:
            final_args_with_params: Dict[str, List[str]] = {}
            self._fill_args_from(
                final_args_with_params, self.config.yarn_dev_tools_arguments, warn_when_overrides=False
            )
            # Add yarndevtools arguments for a specific run
            self._fill_args_from(final_args_with_params, run.yarn_dev_tools_arguments, warn_when_overrides=True)
            run.yarn_dev_tools_arguments = [" ".join([arg, *params]) for arg, params in final_args_with_params.items()]

    @staticmethod
    def _fill_args_from(result: Dict[str, List[str]], arguments: List[str], warn_when_overrides=False):
        for arg in arguments:
            split = arg.split(" ")
            if len(split) == 0:
                raise ValueError("Unexpected argument value: '{}'".format(arg))

            key = split[0]
            if len(split) == 1:
                if warn_when_overrides and key in result:
                    LOG.warning(YARN_DEV_TOOLS_VAR_OVERRIDE_TEMPLATE, key)
                result[key] = []
            else:
                if warn_when_overrides and key in result:
                    LOG.warning(YARN_DEV_TOOLS_VAR_OVERRIDE_TEMPLATE, key)
                result[key] = split[1:]

    def _substitute_regular_variable_in_str(self, orig_value):
        ph = RegularVariables.VAR_PLACEHOLDER
        vars_to_replace = RegularVariables.find_regular_vars_to_replace(orig_value, NORMAL_VAR_MATCHER)
        mod_value = orig_value
        for var in vars_to_replace:
            if var not in self.config.resolved_variables:
                raise ValueError("Variable '{}' is not defined! Original value: {}".format(var, orig_value))
            resolved_var = self.config.resolved_variables[var]
            mod_value = mod_value.replace(f"{ph}{var}{ph}", f"{resolved_var}")
        return mod_value

    def __repr__(self):
        return self.__str__()


class FieldSpecResolver:
    def __init__(self, main_obj):
        self.main_obj = main_obj

    def find_config_attribute_by_field_spec(self, fsi: FieldSpecInstance) -> ResolvedFieldSpec:
        obj = self.main_obj
        parent_obj = None
        attr = None
        for i, attr in enumerate(fsi.fields):
            if isinstance(obj, list):
                list_of_lists = []
                for item in obj:
                    list_of_lists.append(getattr(item, attr))
                obj = list_of_lists
            elif attr.endswith(FieldSpec.MARKER):
                attr = attr[:-2]
                parent_obj = obj
                obj = getattr(obj, attr)
            else:
                match = re.search("([a-zA-Z_]+)\\[(\\d+)]", attr)
                if match:
                    field_name = match.group(1)
                    index = int(match.group(2))
                    parent_obj = getattr(obj, field_name)
                    obj = parent_obj[index]
                elif hasattr(obj, attr):
                    parent_obj = obj
                    obj = getattr(obj, attr)
                else:
                    raise ValueError("Config object has no field with field spec '{}'!", fsi)
        if attr:
            return ResolvedFieldSpec(attr, obj, parent_obj)
        return None


class FieldSpecReplacer:
    def __init__(self, main_obj, field_spec_resolver):
        self.main_obj = main_obj
        self.field_spec_resolver = field_spec_resolver

    def substitute_regular_variables_in_fields(
        self, field_spec_resolver: FieldSpecResolver, field_specs: List[FieldSpec], transformer_func: Callable
    ):
        for field_spec in field_specs:
            fsi = FieldSpecInstance.create_from(field_spec)
            resolved_field_spec = field_spec_resolver.find_config_attribute_by_field_spec(fsi)
            attribute = resolved_field_spec.value
            if isinstance(attribute, list):
                if attribute and isinstance(attribute[0], list):
                    # List of lists
                    for idx, lst in enumerate(attribute):
                        indexed_fsi = FieldSpecInstance.create_from(field_spec, index=idx)
                        self._set_value_to_list_field_spec(lst, indexed_fsi, transformer_func)
                else:
                    self._set_value_to_list_field_spec(attribute, fsi, transformer_func)
            elif isinstance(attribute, str):
                # We don't need recursive substitution here
                mod_value = transformer_func(attribute)
                self.set_config_attribute_by_field_spec(fsi, mod_value)
            else:
                raise ValueError(
                    "Unexpected configuration attribute '{}', object: {}. Expected type of str!".format(
                        field_spec, attribute
                    )
                )

    def set_config_attribute_by_field_spec(self, fsi: FieldSpecInstance, value: Any):
        rfs: ResolvedFieldSpec = self.field_spec_resolver.find_config_attribute_by_field_spec(fsi)
        if value:
            LOG.debug("Setting attribute of object '%s.%s' to value '%s'", rfs.parent, rfs.name, value)
            setattr(rfs.parent, rfs.name, value)

    def _set_value_to_list_field_spec(self, lst, fsi: FieldSpecInstance, transformer_func):
        mod_list = []
        for value in lst:
            mod_list.append(transformer_func(value))
        self.set_config_attribute_by_field_spec(fsi, mod_list)


class EnvironmentVariables:
    def __init__(
        self,
        mandatory_env_vars: List[str],
        optional_env_vars: List[str],
        command_type: CommandType,
        enum_type,
        field_spec_replacer: FieldSpecReplacer,
    ):
        self.valid_env_vars = [e.value for e in enum_type] + [e.value for e in CdswEnvVar]
        self._validate_mandatory_env_var_names(mandatory_env_vars, command_type)
        self._validate_optional_env_var_names(optional_env_vars, command_type)
        self._ensure_if_mandatory_env_vars_are_set(mandatory_env_vars)
        self.field_spec_replacer = field_spec_replacer

    def _validate_optional_env_var_names(self, optional_env_vars, command_type):
        for env_var_name in optional_env_vars:
            if env_var_name not in self.valid_env_vars:
                raise ValueError(
                    "Invalid optional env var specified as '{}'. Valid env vars for Command '{}' are: {}".format(
                        env_var_name, command_type, self.valid_env_vars
                    )
                )

    def _validate_mandatory_env_var_names(self, mandatory_env_vars, command_type: CommandType):
        for env_var_name in mandatory_env_vars:
            if env_var_name not in self.valid_env_vars:
                raise ValueError(
                    "Invalid mandatory env var specified as '{}'. Valid env vars for Command '{}' are: {}".format(
                        env_var_name, command_type, self.valid_env_vars
                    )
                )

    @staticmethod
    def _ensure_if_mandatory_env_vars_are_set(mandatory_env_vars):
        not_found_vars = []
        for env_var in mandatory_env_vars:
            if env_var not in os.environ:
                not_found_vars.append(env_var)

        if not_found_vars:
            raise ValueError("The following env vars are mandatory but they are not set: {}".format(not_found_vars))

    def substitute_env_vars(self, field_spec_resolver: FieldSpecResolver, field_spec: FieldSpec):
        # Separate validation from actual substitution
        not_found_vars = []
        resolved_field_spec = field_spec_resolver.find_config_attribute_by_field_spec(field_spec)
        values = resolved_field_spec.value
        if values and isinstance(values[0], list):
            values = [item for v in values for item in v]
        for arg in values:
            env_vars_to_replace = EnvironmentVariables.find_env_vars_to_replace(arg, ENV_VAR_MATCHER_REGEX)
            for env_var in env_vars_to_replace:
                if env_var not in os.environ:
                    not_found_vars.append(env_var)
        if not_found_vars:
            raise ValueError(
                "The following env vars are optional and they are mapped to YARN dev tools arguments, "
                "so they became mandatory but they are not set: {}".format(not_found_vars)
            )

        def replacer(arg):
            env_vars_to_replace = EnvironmentVariables.find_env_vars_to_replace(arg, ENV_VAR_MATCHER_REGEX)
            env_var_values_for_arg: Dict[str, str] = {}
            for env_var in env_vars_to_replace:
                env_var_values_for_arg[env_var] = os.environ[env_var]
            if env_var_values_for_arg:
                return EnvironmentVariables.replace_env_vars(arg, env_var_values_for_arg)
            else:
                return arg

        # Start substitution
        self.field_spec_replacer.substitute_regular_variables_in_fields(field_spec_resolver, [field_spec], replacer)

    @staticmethod
    def find_env_vars_to_replace(value, regex: str):
        vars_to_replace = []
        for m in re.finditer(regex, value):
            if len(m.groups()) == 1:
                vars_to_replace.append(m.group(1))
            elif "ENV(" in value:
                raise ValueError("Found malformed (empty) variable declaration in string: {}".format(value))
        return vars_to_replace

    @staticmethod
    def replace_env_vars(value, env_var_values: Dict[str, str]):
        # TODO Use  ENV_VAR_MATCHER_REGEX
        for env_name, env_value in env_var_values.items():
            if " " in env_value:
                env_value = f"'{env_value}'"
            value = value.replace(f"ENV({env_name})", env_value)
        return value


class RegularVariables:
    VAR_PLACEHOLDER = "$$"
    BUILT_IN_VARIABLES = {"JOB_START_DATE": DateUtils.get_current_datetime()}

    def __init__(self, orig_vars: Dict[str, str]):
        self.orig_vars = orig_vars
        self.resolved_vars: Dict[str, str] = orig_vars.copy()
        self._add_variables(self.orig_vars)

    def add_more_vars(self, vars_dict: Dict[str, str]):
        self._add_variables(vars_dict)

    def _add_variables(self, var_dict):
        for var_name, raw_var in var_dict.items():
            if self.VAR_PLACEHOLDER in raw_var:
                vars_to_replace = self.find_regular_vars_to_replace(raw_var, NORMAL_VAR_MATCHER)
                modified_var = self._replace_vars(self.resolved_vars, raw_var, vars_to_replace)
                self.resolved_vars[var_name] = modified_var
            elif var_name not in self.resolved_vars:
                self.resolved_vars[var_name] = raw_var
            else:
                prev_value = self.resolved_vars[var_name]
                if prev_value != raw_var:
                    LOG.warning(
                        "Overriding variable '%s'. Previous value: %s, Current value: %s",
                        var_name,
                        self.resolved_vars[var_name],
                        raw_var,
                    )
                    self.resolved_vars[var_name] = raw_var

    @staticmethod
    def find_regular_vars_to_replace(value, matcher: PlaceHolderMatcher):
        indices = [m.span() for m in re.finditer(f"{matcher.format_regex}", value)]
        found_placeholders = re.findall(f"{matcher.escaped_placeholder}", value)
        if len(found_placeholders) % 2 != 0:
            raise ValueError("Malformed variable declaration in string: {}".format(value))
        vars_to_replace = [value[i + matcher.placeholder_len : j - matcher.placeholder_len] for i, j in indices]

        if "" in vars_to_replace:
            raise ValueError("Found malformed (empty) variable declaration in string: {}".format(value))
        return vars_to_replace

    @staticmethod
    def _replace_vars(vars, raw_var, vars_to_replace: List[str]):
        # TODO Use NORMAL_VAR_MATCHER!
        ph = RegularVariables.VAR_PLACEHOLDER
        builtin = RegularVariables.BUILT_IN_VARIABLES
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
                vars[var] = resolved_var
            elif var not in vars:
                # variable does not exist
                raise ValueError("Cannot resolve variable '{}' in raw var: {}".format(var, raw_var))
            elif ph in vars[var]:
                # Variable contains PLACEHOLDERs so it's another variable
                vars_to_replace = RegularVariables.find_regular_vars_to_replace(vars[var], NORMAL_VAR_MATCHER)
                resolved_var = RegularVariables._replace_vars(vars, vars[var], vars_to_replace)
                vars[var] = resolved_var
                raw_var = raw_var.replace(f"{ph}{var}{ph}", f"{resolved_var}")
            else:
                # Simple variable
                resolved_var = vars[var]
                raw_var = raw_var.replace(f"{ph}{var}{ph}", f"{resolved_var}")
        return raw_var
