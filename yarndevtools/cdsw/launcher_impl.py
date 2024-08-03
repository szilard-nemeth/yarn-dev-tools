import enum

from cdswjoblauncher.contract import CdswApp, CdswSetupInput


class CdswExecutionMode(enum.Enum):
    CLOUDERA = "cloudera"
    UPSTREAM = "upstream"


class YarnDevToolsCdswApp(CdswApp):
    def scripts_to_execute(self, cdsw_input: CdswSetupInput):
        try:
            exec_mode = CdswExecutionMode[cdsw_input.execution_mode.upper()]
        except ValueError:
            raise ValueError(f"Invalid value for execution mode: {cdsw_input.execution_mode}")

        if exec_mode == CdswExecutionMode.CLOUDERA:
            return ["clone_upstream_repos.sh", "clone_downstream_repos.sh"]
        elif exec_mode == CdswExecutionMode.UPSTREAM:
            return ["clone_downstream_repos.sh"]
        else:
            raise ValueError(f"Unhandled exec mode: {exec_mode}")
