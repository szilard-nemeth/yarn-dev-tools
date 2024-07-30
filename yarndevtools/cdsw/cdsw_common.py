import logging
from typing import List, Dict

from pythoncommons.file_utils import FileUtils
from pythoncommons.jira_utils import JiraUtils
from pythoncommons.os_utils import OsUtils

from yarndevtools.cdsw.constants import UnitTestResultAggregatorEmailEnvVar
from yarndevtools.constants import UPSTREAM_JIRA_BASE_URL

SKIP_AGGREGATION_DEFAULTS_FILENAME = "skip_aggregation_defaults.txt"
LOG = logging.getLogger(__name__)



class GenericCdswConfigUtils:
    @staticmethod
    def quote_list_items(lst):
        return " ".join(f'"{w}"' for w in lst)

    @staticmethod
    def quote(val):
        if '"' in val:
            return val
        return '"' + val + '"'

    @staticmethod
    def unquote(val):
        return val.strip('"')


class JiraUmbrellaDataFetcherCdswUtils:
    @staticmethod
    def fetch_umbrella_titles(jira_ids: List[str]) -> Dict[str, str]:
        return {j_id: JiraUmbrellaDataFetcherCdswUtils._fetch_umbrella_title(j_id) for j_id in jira_ids}

    @staticmethod
    def _fetch_umbrella_title(jira_id: str):
        jira_html_file = f"/tmp/jira_{jira_id}.html"
        LOG.info("Fetching HTML of jira: %s", jira_id)
        jira_html = JiraUtils.download_jira_html(UPSTREAM_JIRA_BASE_URL, jira_id, jira_html_file)
        return JiraUtils.parse_jira_title(jira_html)


class UnitTestResultAggregatorCdswUtils:
    DEFAULT_SKIP_LINES_STARTING_WITH = ["Failed testcases:", "Failed testcases (", "FILTER:", "Filter expression: "]

    @classmethod
    def determine_lines_to_skip(cls) -> List[str]:
        skip_lines_starting_with: List[str] = cls.DEFAULT_SKIP_LINES_STARTING_WITH
        # If env var "SKIP_AGGREGATION_RESOURCE_FILE" is specified, try to read file
        # The file takes precedence over the default list of DEFAULT_SKIP_LINES_STARTING_WITH
        skip_aggregation_res_file = OsUtils.get_env_value(
            UnitTestResultAggregatorEmailEnvVar.SKIP_AGGREGATION_RESOURCE_FILE.value
        )
        skip_aggregation_res_file_auto_discovery_str = OsUtils.get_env_value(
            UnitTestResultAggregatorEmailEnvVar.SKIP_AGGREGATION_RESOURCE_FILE_AUTO_DISCOVERY.value
        )
        LOG.info(
            "Value of env var '%s': %s",
            UnitTestResultAggregatorEmailEnvVar.SKIP_AGGREGATION_RESOURCE_FILE_AUTO_DISCOVERY.value,
            skip_aggregation_res_file_auto_discovery_str,
        )

        # TODO Bool parsing should be done in get_env_value
        if skip_aggregation_res_file_auto_discovery_str in ("True", "true", "1"):
            skip_aggregation_res_file_auto_discovery = True
        elif skip_aggregation_res_file_auto_discovery_str in ("False", "false", "0"):
            skip_aggregation_res_file_auto_discovery = False
        else:
            raise ValueError(
                "Invalid value for environment variable '{}': {}".format(
                    UnitTestResultAggregatorEmailEnvVar.SKIP_AGGREGATION_RESOURCE_FILE_AUTO_DISCOVERY.value,
                    skip_aggregation_res_file_auto_discovery_str,
                )
            )

        if skip_aggregation_res_file_auto_discovery:
            found_with_auto_discovery = cls._auto_discover_skip_aggregation_result_file()
            if found_with_auto_discovery:
                LOG.info("Found Skip aggregation resource file with auto-discovery: %s", found_with_auto_discovery)
                return FileUtils.read_file_to_list(found_with_auto_discovery)
        elif skip_aggregation_res_file:
            LOG.info("Trying to check specified skip aggregation resource file: %s", skip_aggregation_res_file)
            FileUtils.ensure_is_file(skip_aggregation_res_file)
            return FileUtils.read_file_to_list(skip_aggregation_res_file)
        return skip_lines_starting_with

    @classmethod
    # TODO cdsw-separation yarndevtools specific
    def _auto_discover_skip_aggregation_result_file(cls):
        found_with_auto_discovery: str or None = None
        # TODO cdsw-separation should be imported from CDSW job launcher
        search_basedir = CommonDirs.MODULE_ROOT
        LOG.info("Looking for file '%s' in basedir: %s", SKIP_AGGREGATION_DEFAULTS_FILENAME, search_basedir)
        results = FileUtils.search_files(search_basedir, SKIP_AGGREGATION_DEFAULTS_FILENAME)
        if not results:
            LOG.warning(
                "Skip aggregation resource file auto-discovery is enabled, "
                "but failed to find file '%s' from base directory '%s'.",
                SKIP_AGGREGATION_DEFAULTS_FILENAME,
                search_basedir,
            )
        elif len(results) > 1:
            LOG.warning(
                "Skip aggregation resource file auto-discovery is enabled, "
                "but multiple files found from base directory '%s'. Found files: %s",
                SKIP_AGGREGATION_DEFAULTS_FILENAME,
                search_basedir,
                results,
            )
        else:
            found_with_auto_discovery = results[0]
        return found_with_auto_discovery
