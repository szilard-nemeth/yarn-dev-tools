import logging
from typing import Dict, Tuple

from pythoncommons.file_utils import FileUtils
from pythoncommons.process import CommandRunner

from yarndevtools.commands.branchcomparator.common import BranchType, BranchData

LOG = logging.getLogger(__name__)


class LegacyScriptRunner:
    @staticmethod
    def start(config, branches, repo_path):
        script_results: Dict[BranchType, Tuple[str, str]] = LegacyScriptRunner._execute_compare_script(
            config, branches, working_dir=repo_path
        )
        for br_type in BranchType:
            branch_data = branches.get_branch(br_type)
            branch_data.unique_jira_ids_legacy_script = LegacyScriptRunner._get_unique_jira_ids_for_branch(
                script_results, branch_data
            )
            LOG.debug(
                f"[LEGACY SCRIPT] Unique commit results for {br_type.value}: "
                f"{branch_data.unique_jira_ids_legacy_script}"
            )

        # Cross check unique jira ids with previous results
        for br_type in BranchType:
            branch_data = branches.get_branch(br_type)
            # TODO this seems to be completely wrong branches.summary.unique_commits is no longer stored there
            unique_jira_ids = [c.jira_id for c in branches.summary.unique_commits[br_type]]
            if LOG.isEnabledFor(logging.DEBUG):
                LOG.debug(
                    f"[CURRENT SCRIPT] Found unique commits on branch "
                    f"'{branch_data.name}' [{br_type}]: {unique_jira_ids} "
                )
            else:
                LOG.debug(
                    f"[CURRENT SCRIPT] Found unique commits on branch "
                    f"'{branch_data.name}' [{br_type}]: {len(unique_jira_ids)} "
                )

    @staticmethod
    def _get_unique_jira_ids_for_branch(script_results: Dict[BranchType, Tuple[str, str]], branch_data: BranchData):
        branch_type = branch_data.type
        res_tuple = script_results[branch_type]
        LOG.info(f"CLI Command for {branch_type} was: {res_tuple[0]}")
        LOG.info(f"Output of command for {branch_type} was: {res_tuple[1]}")
        lines = res_tuple[1].splitlines()
        unique_jira_ids = [line.split(" ")[0] for line in lines]

        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug(
                f"[LEGACY SCRIPT] Found unique commits on branch "
                f"'{branch_data.name}' [{branch_type}]: {unique_jira_ids}"
            )
        else:
            LOG.debug(
                f"[LEGACY SCRIPT] Found unique commits on branch "
                f"'{branch_data.name}' [{branch_type}]: {len(unique_jira_ids)}"
            )

        return unique_jira_ids

    @staticmethod
    def _execute_compare_script(config, branches, working_dir) -> Dict[BranchType, Tuple[str, str]]:
        compare_script = config.legacy_compare_script_path
        master_br_name = branches.get_branch(BranchType.MASTER).shortname
        feature_br_name = branches.get_branch(BranchType.FEATURE).shortname
        output_dir = FileUtils.join_path(config.output_dir, "git_compare_script_output")
        FileUtils.ensure_dir_created(output_dir)

        results: Dict[BranchType, Tuple[str, str]] = {
            BranchType.MASTER: LegacyScriptRunner._exec_script_only_on_master(
                compare_script, feature_br_name, master_br_name, output_dir, working_dir
            ),
            BranchType.FEATURE: LegacyScriptRunner._exec_script_only_on_feature(
                compare_script, feature_br_name, master_br_name, output_dir, working_dir
            ),
        }
        return results

    @staticmethod
    def _exec_script_only_on_master(compare_script, feature_br_name, master_br_name, output_dir, working_dir):
        args1 = f"{feature_br_name} {master_br_name}"
        output_file1 = FileUtils.join_path(output_dir, f"only-on-{master_br_name}")
        cli_cmd, cli_output = CommandRunner.execute_script(
            compare_script, args=args1, working_dir=working_dir, output_file=output_file1, use_tee=True
        )
        return cli_cmd, cli_output

    @staticmethod
    def _exec_script_only_on_feature(compare_script, feature_br_name, master_br_name, output_dir, working_dir):
        args2 = f"{master_br_name} {feature_br_name}"
        output_file2 = FileUtils.join_path(output_dir, f"only-on-{feature_br_name}")
        cli_cmd, cli_output = CommandRunner.execute_script(
            compare_script, args=args2, working_dir=working_dir, output_file=output_file2, use_tee=True
        )
        return cli_cmd, cli_output
