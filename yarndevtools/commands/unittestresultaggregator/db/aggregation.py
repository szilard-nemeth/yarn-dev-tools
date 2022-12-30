import datetime
from pprint import pformat
from typing import Dict, Set, List, Tuple

from pythoncommons.date_utils import DateUtils

from yarndevtools.commands.unittestresultaggregator.common.model import FailedBuildAbs
from yarndevtools.commands.unittestresultaggregator.db.model import EmailContent, LOG
from yarndevtools.commands.unittestresultaggregator.db.persistence import UTResultAggregatorDatabase
from yarndevtools.common.common_model import JobBuildData


class JenkinsJobBuildDataAndEmailContentAggregator:
    from yarndevtools.commands.unittestresultaggregator.common.aggregation import AggregationResults

    def __init__(self, db: UTResultAggregatorDatabase):
        self._db = db
        # key: job name, value: Dict[build number, JobBuildData]
        self.fetcher_data_dict: Dict[str, Dict[str, JobBuildData]] = {}
        # key: job name, value: Dict[build number, EmailContent]
        self.aggregator_data_dict: Dict[str, Dict[str, EmailContent]] = {}
        # key: job name, value: set of dates
        self.dates: Dict[str, Set[datetime.date]] = {}

    def aggregate(self, result: AggregationResults):
        """
        An email can be sent for the same build many times.
        Example: https://build.infra.cloudera.com/job/Mawo-UT-hadoop-CDPD-7.x/427/
        Assuming that the email body is the same for multiple sends for the same job instance.
        Losing data here is fine as only one instance of a (job_name, build_number) should be kept.
        So, if build number was previously saved to dict, we don't save subsequent builds.
        This is true for both iterations below.
        :param result:
        :return:
        """
        # Data from UT result fetcher: failed jenkins builds
        build_data: List[JobBuildData] = self._db.find_and_validate_all_build_data()

        # Data from UT result aggregator: email contents
        email_contents: List[EmailContent] = self._db.find_and_validate_all_email_content()

        processed: Set[Tuple[str, str]] = set()  # tuple: (job name, build number)
        # TODO yarndevtoolsv2: Could simplify code if JobBuildData and EmailContent had a common interface for used fields
        for bd in build_data:
            builds_per_job = self.fetcher_data_dict.setdefault(bd.job_name, {})

            if (bd.job_name, bd.build_number) not in processed:
                builds_per_job.setdefault(bd.build_number, bd)
                self.dates.setdefault(bd.job_name, set()).add(bd.build_datetime.date())
                processed.add((bd.job_name, bd.build_number))
            else:
                LOG.debug("%s is already processed, not storing build again")

        if not email_contents:
            raise ValueError(
                "Loaded email contents from DB is empty! Please fill DB by running in execution mode: EMAIL_ONLY"
            )

        for ec in email_contents:
            LOG.debug("Processing email content for aggregation: %s", ec.build_url)
            builds_per_job = self.aggregator_data_dict.setdefault(ec.job_name, {})

            key = (ec.job_name, ec.build_number)
            if key not in processed:
                builds_per_job.setdefault(ec.build_number, ec)
                self.dates.setdefault(ec.job_name, set()).add(ec.date.date())
                processed.add(key)
            else:
                LOG.debug("%s is already processed, not storing build again", key)

        self._do_aggregate(result)

    def _do_aggregate(self, result: AggregationResults):
        self.fetcher_only: Dict[str, Set[str]] = self._get_only_in_first_dict(
            self.fetcher_data_dict, self.aggregator_data_dict
        )
        self.aggregator_only: Dict[str, Set[str]] = self._get_only_in_first_dict(
            self.aggregator_data_dict, self.fetcher_data_dict
        )
        self.from_both: Dict[str, Set[str]] = self._get_from_both_dicts(
            self.fetcher_data_dict, self.aggregator_data_dict
        )

        processed: Set[Tuple[str, str]] = set()  # tuple: (job name, build number)

        self._print_stats()

        # TODO avoid code duplication
        for job_name, inner_dict in self.aggregator_data_dict.items():
            for build_number, job_build_data in inner_dict.items():
                item: EmailContent = self.aggregator_data_dict[job_name][build_number]
                self._process_failed_build(result, FailedBuildAbs.create_from_email(item))
                processed.add((job_name, build_number))

        for job_name, inner_dict in self.fetcher_data_dict.items():
            for build_number, job_build_data in inner_dict.items():
                if (job_name, build_number) not in processed:
                    item: JobBuildData = self.fetcher_data_dict[job_name][build_number]
                    self._process_failed_build(result, FailedBuildAbs.create_from_job_build_data(item))
                else:
                    pass
                    # TODO yarndevtoolsv2: Log something

        result.finish_processing()

        # TODO yarndevtoolsv2: These 2 should be the same, printout should not happen in this class!
        #  DEBUG why these 2 dicts are not equal!
        #  Save email meta to DB // store dates of emails as well to mongodb:
        #  Write start date, end date, missing dates between start and end date
        #  builds_with_dates = self._aggregation_results._failed_builds.get_dates()
        #  Cross-check date-related functionality with JenkinsJobBuildDataAndEmailContentJoiner

        tmp = {}
        for job_name, dates in self.dates.items():
            tmp[job_name] = sorted(list(dates), reverse=True)
        self.dates = tmp

        dates1 = result._aggregation_results._failed_builds.get_unique_dates()
        dates2 = self.dates
        assert dates1 == dates2, "Date dictionaries are not equal"

    def _is_aggregator_only(self, job_name, build_number):
        if job_name in self.aggregator_only and build_number in self.aggregator_only[job_name]:
            return True
        return False

    def _is_fetcher_only(self, job_name, build_number):
        if job_name in self.fetcher_only and build_number in self.fetcher_only[job_name]:
            return True
        return False

    @staticmethod
    # TODO: Make this more abstract and move to pythoncommons?
    def _get_only_in_first_dict(dic_a, dic_b):
        res: Dict[str, Set[str]] = {}
        for job_name, inner_dict in dic_a.items():
            build_numbers_a = inner_dict.keys()
            if job_name not in dic_b:
                res[job_name] = set(build_numbers_a)
            else:
                build_numbers_b = dic_b[job_name].keys()
                for build_number in build_numbers_a:
                    if build_number not in build_numbers_b:
                        res.setdefault(job_name, set()).add(build_number)
        return res

    @staticmethod
    # TODO: this more abstract and move to pythoncommons?
    def _get_from_both_dicts(dic_a, dic_b):
        res: Dict[str, Set[str]] = {}
        for job_name, inner_dict in dic_a.items():
            build_numbers_a = inner_dict.keys()
            if job_name in dic_b:
                res[job_name] = set()

            build_numbers_b = dic_b[job_name].keys()
            for build_number in build_numbers_a:
                if build_number in build_numbers_b:
                    res.setdefault(job_name, set()).add(build_number)
        return res

    def _print_stats(self):
        self._print_date_stats()
        aggregator_only_formatted = pformat(self.aggregator_only)
        fetcher_only_formatted = pformat(self.fetcher_only)
        from_both_formatted = pformat(self.from_both)
        LOG.debug(
            "Printing found testcase failure statistics: \n"
            "Aggregator only: %s\n"
            "Fetcher only: %s\n"
            "From both: %s",
            aggregator_only_formatted,
            fetcher_only_formatted,
            from_both_formatted,
        )

    def _print_date_stats(self):
        LOG.debug("Printing date statistics. ")
        for job_name, dates in self.dates.items():
            sorted_dates = sorted(dates)
            first_date = sorted_dates[0]
            last_date = sorted_dates[-1]
            all_no_of_days = (last_date - first_date).days
            missing_dates = DateUtils.get_missing_dates(sorted_dates)
            LOG.debug(
                "Job: %s, All days: %s, First date: %s, Last date: %s, Sorted dates: %s, Missing dates: %s",
                job_name,
                all_no_of_days,
                first_date,
                last_date,
                pformat(sorted_dates),
                pformat(missing_dates),
            )

    @staticmethod
    def _process_failed_build(result: AggregationResults, failed_build: FailedBuildAbs):
        LOG.debug("Processing failed build: %s", failed_build.origin())
        result.start_new_context()
        result.match_testcases(failed_build)
        result.finish_context(failed_build)
