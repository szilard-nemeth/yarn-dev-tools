from typing import List

from pythoncommons.date_utils import DateUtils

from yarndevtools.commands.unittestresultaggregator.common import LOG
from yarndevtools.commands.unittestresultaggregator.common_tmp.model import KnownTestFailureInJira


class KnownTestFailures:
    def __init__(self, gsheet_wrapper=None, gsheet_jira_table=None):
        self._testcases_to_jiras: List[KnownTestFailureInJira] = []
        self.gsheet_wrapper = gsheet_wrapper
        if gsheet_jira_table:
            self._testcases_to_jiras: List[KnownTestFailureInJira] = self._load_and_convert_known_test_failures_in_jira(
                gsheet_jira_table
            )
        self._index = 0
        self._num_testcases = len(self._testcases_to_jiras)

    def __len__(self):
        return self._num_testcases

    def __iter__(self):
        return self

    def __next__(self):
        if self._index == self._num_testcases:
            raise StopIteration
        result = self._testcases_to_jiras[self._index]
        self._index += 1
        return result

    def _load_and_convert_known_test_failures_in_jira(self, gsheet_jira_table) -> List[KnownTestFailureInJira]:
        # TODO yarndevtoolsv2: Data should be written to mongoDB once
        raw_data_from_gsheet = self.gsheet_wrapper.read_data(gsheet_jira_table, "A1:E150")
        LOG.info(f"Successfully loaded data from worksheet: {gsheet_jira_table}")

        header: List[str] = raw_data_from_gsheet[0]
        expected_header = ["Testcase", "Jira", "Resolution date"]
        if header != expected_header:
            raise ValueError(
                "Detected suspicious known test failures table header. "
                f"Expected header: {expected_header}, "
                f"Current header: {header}"
            )

        raw_data_from_gsheet = raw_data_from_gsheet[1:]
        known_tc_failures = []
        for row in raw_data_from_gsheet:
            self._preprocess_row(row)
            t_name = row[0]
            jira_link = row[1]
            date_time = DateUtils.convert_to_datetime(row[2], "%m/%d/%Y") if row[2] else None
            known_tc_failures.append(KnownTestFailureInJira(t_name, jira_link, date_time))

        return known_tc_failures

    @staticmethod
    def _preprocess_row(row):
        row_len = len(row)
        if row_len < 2:
            raise ValueError(
                "Both 'Testcase' and 'Jira' are mandatory items but row does not contain them. "
                f"Problematic row: {row}"
            )
        # In case of 'Resolution date' is missing, append an empty-string so that all rows will have
        # an equal number of cells. This eases further processing.
        if row_len == 2:
            row.append("")
