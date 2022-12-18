import logging
import os
import re
from copy import copy
from dataclasses import dataclass, field
from typing import List, Dict, Any, Callable, Union

from dacite import from_dict
from pythoncommons.date_utils import DateUtils
from pythoncommons.string_utils import auto_str

from yarndevtools.cdsw.constants import (
    JiraUmbrellaFetcherEnvVar,
    BranchComparatorEnvVar,
    UnitTestResultFetcherEnvVar,
    UnitTestResultAggregatorEmailEnvVar,
    ReviewSheetBackportUpdaterEnvVar,
    ReviewSyncEnvVar,
    CdswEnvVar,
)
from yarndevtools.common.shared_command_utils import CommandType

YARN_DEV_TOOLS_VAR_OVERRIDE_TEMPLATE = "Found argument in yarn_dev_tools_arguments and runconfig.yarn_dev_tools_arguments: '%s'. The latter will take predence."
JOB_START_DATE_KEY = "JOB_START_DATE"
LOG = logging.getLogger(__name__)


class Include(object):
    @staticmethod
    def when(expression, if_block, else_block={}):
        return if_block if expression else else_block


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


class FieldSpecResolver:
    def __init__(self, main_obj):
        self.main_obj = main_obj

    def find_attribute_by_field_spec(self, fsi: FieldSpecInstance) -> ResolvedFieldSpec:
        obj = self.main_obj
        parent_obj = None
        attr = None
        for i, attr in enumerate(fsi.fields):
            if isinstance(obj, list):
                list_of_lists = []
                for item in obj:
                    list_of_lists.append(getattr(item, attr))
                parent_obj = obj
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
            rfs = ResolvedFieldSpec(name=attr, value=obj, parent=parent_obj)
            value_list = isinstance(rfs.value, list)
            parent_list = isinstance(rfs.parent, list)
            if parent_list and not value_list:
                raise ValueError(
                    "Invalid configuration for Field spec instance {}. If parent is a list, values should be a list as well!"
                    "Resolved field spec: {}".format(fsi, rfs)
                )
            if parent_list and value_list and len(rfs.parent) != len(rfs.value):
                raise ValueError(
                    "Invalid configuration for Field spec instance {}. Parent object list should be the same length of value list!"
                    "Resolved field spec: {}".format(fsi, rfs)
                )
            return rfs
        return None


class FieldSpecReplacer:
    @staticmethod
    def substitute_regular_variables_in_fields(
        cdsw_config, fieldspec_resolver: FieldSpecResolver, field_specs: List[FieldSpec]
    ):
        for field_spec in field_specs:
            fsi = FieldSpecInstance.create_from(field_spec)
            rfs = fieldspec_resolver.find_attribute_by_field_spec(fsi)
            field_value = rfs.value
            LOG.debug("Field spec: %s, Resolved field spec:%s", field_spec, rfs)

            if isinstance(field_value, list):
                if field_value and isinstance(field_value[0], list):
                    # List of lists
                    for idx, lst in enumerate(field_value):
                        indexed_fsi = FieldSpecInstance.create_from(field_spec, index=idx)
                        rfs: ResolvedFieldSpec = fieldspec_resolver.find_attribute_by_field_spec(indexed_fsi)
                        FieldSpecReplacer._set_value_to_list_field_spec(indexed_fsi, rfs, lst, cdsw_config)
                else:
                    FieldSpecReplacer._set_value_to_list_field_spec(fsi, rfs, field_value, cdsw_config)
            elif isinstance(field_value, dict):
                for k, v in field_value.items():
                    field_value[k] = cdsw_config.resolve_lambda(v, rfs)
            elif isinstance(field_value, Callable):
                FieldSpecReplacer.set_config_attribute_by_field_spec(
                    fsi, rfs, cdsw_config.resolve_lambda(field_value, rfs)
                )
            else:
                raise ValueError(
                    "Unexpected configuration field_value '{}', object: {}. Expected type of these: {}!".format(
                        field_spec, field_value, [list, str, dict]
                    )
                )

    @staticmethod
    def set_config_attribute_by_field_spec(fsi: FieldSpecInstance, rfs: ResolvedFieldSpec, value: Any):
        if not value:
            LOG.warning("Tried to set None value to field spec: %s", fsi)
            return

        LOG.debug("Field spec: %s, Resolved field spec:%s", fsi, rfs)

        if isinstance(rfs.parent, list):
            if not isinstance(value, list):
                raise ValueError(
                    "Expected a value list if parent is a list on the ResolvedFieldSpec: {}. Value: {}".format(
                        rfs, value
                    )
                )
            for parent_obj, new_value in zip(rfs.parent, value):
                LOG.debug(
                    "Setting attribute of object '%s.%s' to value '%s' (original value was: %s)",
                    rfs.parent,
                    rfs.name,
                    new_value,
                    rfs.value,
                )
                setattr(parent_obj, rfs.name, new_value)
        else:
            LOG.debug(
                "Setting attribute of object '%s.%s' to value '%s' (original value was: %s)",
                rfs.parent,
                rfs.name,
                value,
                rfs.value,
            )
            setattr(rfs.parent, rfs.name, value)

    @staticmethod
    def _set_value_to_list_field_spec(fsi: FieldSpecInstance, rfs: ResolvedFieldSpec, lst, cdsw_config):
        mod_list = []
        for value in lst:
            mod_list.append(cdsw_config.resolve_lambda(value, rfs))
        FieldSpecReplacer.set_config_attribute_by_field_spec(fsi, rfs, mod_list)


@dataclass
class EmailSettings:
    enabled: bool
    send_attachment: bool
    attachment_file_name: Union[str, Callable]
    email_body_file_from_command_data: Union[str, Callable]
    subject: Union[str, Callable]
    sender: Union[str, Callable]


@dataclass
class DriveApiUploadSettings:
    enabled: bool
    file_name: Union[str, Callable]


@dataclass
class CdswRun:
    name: str
    email_settings: Union[EmailSettings, None]
    drive_api_upload_settings: Union[DriveApiUploadSettings, None]
    yarn_dev_tools_arguments: List[Union[str, Callable]] = field(default_factory=list)
    variables: Dict[str, Union[str, Callable]] = field(default_factory=dict)


@dataclass
class CdswJobConfig:
    job_name: str
    command_type: CommandType
    runs: Union[List[CdswRun], Callable] = field(default_factory=list)
    mandatory_env_vars: List[str] = field(default_factory=list)
    optional_env_vars: List[str] = field(default_factory=list)
    yarn_dev_tools_arguments: List[Union[str, Callable]] = field(default_factory=list)
    global_variables: Dict[str, Union[str, bool, int, Callable]] = field(default_factory=dict)
    env_sanitize_exceptions: List[str] = field(default_factory=list)

    # Dynamic
    runs_defined_as_callable: bool = False

    def __post_init__(self):
        self.resolver: Resolver = None

    @staticmethod
    def job_start_date():
        return GlobalVariables.job_start_date()

    def var(self, var_name):
        return self.resolver.var(var_name)

    def env(self, env_name: str):
        return self.resolver.env(env_name)

    def env_or_default(self, env_name: str, default: str):
        return self.resolver.env_or_default(env_name, default)

    def resolve_lambda(self, callable, rfs: ResolvedFieldSpec):
        return self.resolver.resolve_lambda(callable, rfs)


@auto_str
class CdswJobConfigReader:
    command_to_env_var_class = {
        CommandType.JIRA_UMBRELLA_DATA_FETCHER: JiraUmbrellaFetcherEnvVar,
        CommandType.BRANCH_COMPARATOR: BranchComparatorEnvVar,
        CommandType.UNIT_TEST_RESULT_FETCHER: UnitTestResultFetcherEnvVar,
        CommandType.UNIT_TEST_RESULT_AGGREGATOR_EMAIL: UnitTestResultAggregatorEmailEnvVar,
        # TODO yarndevtoolsv2
        CommandType.UNIT_TEST_RESULT_AGGREGATOR_DB: UnitTestResultAggregatorEmailEnvVar,
        CommandType.REVIEW_SHEET_BACKPORT_UPDATER: ReviewSheetBackportUpdaterEnvVar,
        CommandType.REVIEWSYNC: ReviewSyncEnvVar,
    }

    @staticmethod
    def read_from_file(file):
        if not file:
            raise ValueError("Config file must be specified!")
        config_reader = CdswJobConfigReader()
        conf_dict = config_reader._read_from_python_conf(file)
        config = from_dict(data_class=CdswJobConfig, data=conf_dict)
        config_reader.process_config(config)
        return config

    def _read_from_python_conf(self, file):
        cdswconfig_module = self._load_module(file)
        job_config: Dict[Any, Any] = cdswconfig_module.config
        LOG.info("Job config: %s", job_config)
        return job_config

    @staticmethod
    def _load_module(file):
        import importlib.util

        spec = importlib.util.spec_from_file_location("cdswconfig", file)
        cdswconfig_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cdswconfig_module)
        return cdswconfig_module

    def process_config(self, config: CdswJobConfig):
        # Pre-initialize
        config.runs_defined_as_callable = isinstance(config.runs, Callable)
        config.resolver = Resolver(config)

        # Validatation
        LOG.info("Validating config: %s", config)
        if not config.runs:
            raise ValueError("Section 'runs' must be defined and cannot be empty!")
        self._validate_run_names(config)

        # Post-initialize
        enum_type = self.command_to_env_var_class[config.command_type]
        EnvironmentVariables(
            config.mandatory_env_vars,
            config.optional_env_vars,
            config.command_type,
            enum_type,
        )
        config.resolver.resolve_vars()
        self._generate_runs_if_required(config)
        self._finalize_yarn_dev_tools_arguments(config)

    @staticmethod
    def _validate_run_names(config, force_validate=False):
        names = set()
        if config.runs_defined_as_callable and not force_validate:
            return
        for run in config.runs:
            if run.name in names:
                raise ValueError("Duplicate job name not allowed! Job name: {}".format(run.name))
            names.add(run.name)

    def _generate_runs_if_required(self, config):
        if config.runs_defined_as_callable:
            run_dicts = config.runs(config)
            runs = []
            for run_dict in run_dicts:
                runs.append(from_dict(data_class=CdswRun, data=run_dict))
            config.runs = runs
            self._validate_run_names(config, force_validate=True)

            FieldSpecReplacer.substitute_regular_variables_in_fields(
                config,
                config.resolver._field_spec_resolver,
                Resolver.FIELD_SUBSTITUTION_PHASE2_DYNAMIC_RUN_CONFIG,
            )

    def _finalize_yarn_dev_tools_arguments(self, config):
        for run in config.runs:
            final_args_with_params: Dict[str, List[str]] = {}
            self._fill_args_from(final_args_with_params, config.yarn_dev_tools_arguments, warn_when_overrides=False)
            # Add yarndevtools arguments for a specific run
            self._fill_args_from(final_args_with_params, run.yarn_dev_tools_arguments, warn_when_overrides=True)
            run.yarn_dev_tools_arguments = [" ".join([arg, *params]) for arg, params in final_args_with_params.items()]

    @staticmethod
    def _fill_args_from(result: Dict[str, List[str]], arguments: List[str], warn_when_overrides=False):
        for arg in arguments:
            if arg == "":
                continue
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

    def __repr__(self):
        return self.__str__()


class Resolver:
    _FIELD_SUBSTITIONS_RUN_FIELDS = [
        FieldSpec("runs[].email_settings.subject"),
        FieldSpec("runs[].email_settings.sender"),
        FieldSpec("runs[].email_settings.attachment_file_name"),
        FieldSpec("runs[].email_settings.email_body_file_from_command_data"),
        FieldSpec("runs[].drive_api_upload_settings.file_name"),
        FieldSpec("runs[].yarn_dev_tools_arguments"),
        FieldSpec("runs[].variables"),
    ]

    _DEFAULT_VARIABLE_SUBSTITUTION_FIELDS = [
        FieldSpec("global_variables"),
        *_FIELD_SUBSTITIONS_RUN_FIELDS,
        FieldSpec("yarn_dev_tools_arguments"),
    ]

    _FIELD_SUBSTITUTION_PHASE1_DYNAMIC_RUN_CONFIG = [
        FieldSpec("global_variables"),
        FieldSpec("yarn_dev_tools_arguments"),
    ]

    FIELD_SUBSTITUTION_PHASE2_DYNAMIC_RUN_CONFIG = [*_FIELD_SUBSTITIONS_RUN_FIELDS]

    def __init__(self, config):
        self._current_rfs = None
        self.config = config
        self.global_variables = GlobalVariables(config.global_variables)
        self.env_sanitize_exceptions = config.env_sanitize_exceptions

        # Dynamic
        self._field_spec_resolver = FieldSpecResolver(config)

    def resolve_vars(self):
        fields_to_resolve = self._DEFAULT_VARIABLE_SUBSTITUTION_FIELDS
        if self.config.runs_defined_as_callable:
            fields_to_resolve = self._FIELD_SUBSTITUTION_PHASE1_DYNAMIC_RUN_CONFIG
        FieldSpecReplacer.substitute_regular_variables_in_fields(
            self.config,
            self._field_spec_resolver,
            fields_to_resolve,
        )

    def var(self, var_name):
        resolution_context = self._current_rfs.name
        if resolution_context == "global_variables":
            val = self._resolve_from_global(var_name, resolution_context)
            if isinstance(val, Callable):
                return self.resolve_lambda(val, self._current_rfs)
            if val is not None:
                return val
        elif resolution_context == "variables" and type(self._current_rfs.parent == CdswRun):
            return self._resolve_from_global(var_name, resolution_context)
        elif resolution_context == "yarn_dev_tools_arguments" and type(self._current_rfs.parent == CdswRun):
            cdsw_run = self._current_rfs.parent
            val = self._resolve_from_variables(cdsw_run, var_name, resolution_context)
            if isinstance(val, Callable):
                return self.resolve_lambda(val, self._current_rfs)
            if val:
                return val
            return self._resolve_from_global(var_name, resolution_context)
        else:
            return self._resolve_from_global(var_name, resolution_context)

    def _resolve_from_global(self, var_name, resolution_context: str):
        LOG.debug("Resolving variable '%s' from '%s'", var_name, resolution_context)
        if var_name in self.global_variables.vars:
            return self.global_variables.vars[var_name]
        raise ValueError("Cannot resolve variable '{}' in: {}".format(var_name, resolution_context))

    def _resolve_from_variables(self, cdsw_run, var_name, resolution_context: str):
        LOG.debug("Resolving variable '%s' from '%s'", var_name, resolution_context)
        if not hasattr(cdsw_run, "variables"):
            return None
        if var_name in cdsw_run.variables:
            return cdsw_run.variables[var_name]
        # TODO raise exception if not found?

    def resolve_lambda(self, callable, rfs):
        self._current_rfs = rfs
        if not isinstance(callable, Callable):
            return callable
        return callable(self.config)

    def env(self, env_name):
        env_value = os.getenv(env_name)
        if not env_value:
            raise ValueError("The following env var is not set: {}".format(env_name))
        return EnvironmentVariables.sanitize_env_value(env_name, env_value, self.env_sanitize_exceptions)

    def env_or_default(self, env_name, default):
        env_value = os.getenv(env_name)
        if env_value:
            return EnvironmentVariables.sanitize_env_value(env_name, env_value, self.env_sanitize_exceptions)
        return default


class GlobalVariables:
    BUILT_IN_VARIABLES = {JOB_START_DATE_KEY: DateUtils.get_current_datetime()}

    def __init__(self, orig_vars: Dict[str, str]):
        self._validate_vars_not_built_in(orig_vars)
        self.vars = orig_vars

    @staticmethod
    def _validate_vars_not_built_in(orig_vars):
        builtins = GlobalVariables.BUILT_IN_VARIABLES
        for var_name in orig_vars:
            if var_name in builtins:
                raise ValueError(
                    "Cannot use variables with the same name as built-in variables. "
                    "Built-ins: {}"
                    "Current var: {}".format(builtins, var_name)
                )

    @staticmethod
    def job_start_date():
        return GlobalVariables.BUILT_IN_VARIABLES[JOB_START_DATE_KEY]


class EnvironmentVariables:
    def __init__(
        self,
        mandatory_env_vars: List[str],
        optional_env_vars: List[str],
        command_type: CommandType,
        enum_type,
    ):
        self.valid_env_vars = [e.value for e in enum_type] + [e.value for e in CdswEnvVar]
        self._validate_mandatory_env_var_names(mandatory_env_vars, command_type)
        self._validate_optional_env_var_names(optional_env_vars, command_type)
        self._ensure_if_mandatory_env_vars_are_set(mandatory_env_vars)

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

    @staticmethod
    def sanitize_env_value(env_name, env_value, sanitize_exceptions):
        if env_name in sanitize_exceptions:
            LOG.debug("Won't sanitize env var '%s' as per configuration!", env_name)
            return env_value
        has_quote_or_single_quote = True if "'" in env_value or '"' in env_value else False
        if " " in env_value and not has_quote_or_single_quote:
            env_value = '"' + env_value + '"'
        return env_value
