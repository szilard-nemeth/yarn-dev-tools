from cdswjoblauncher.contract import CdswApp, CdswSetupInput


class YarnDevToolsCdswApp(CdswApp):
    def scripts_to_execute(self, cdsw_input: CdswSetupInput):
        raise NotImplementedError()
