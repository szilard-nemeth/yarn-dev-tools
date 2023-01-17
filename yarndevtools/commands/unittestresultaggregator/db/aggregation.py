import datetime
from pprint import pformat
from typing import Dict, Set, List, Tuple

from pythoncommons.date_utils import DateUtils

from yarndevtools.commands.unittestresultaggregator.common.model import FailedBuildAbs
from yarndevtools.commands.unittestresultaggregator.db.model import EmailContent, LOG
from yarndevtools.commands.unittestresultaggregator.db.persistence import UTResultAggregatorDatabase
from yarndevtools.common.common_model import JobBuildData, AggregatorEntity


class JenkinsJobBuildDataAndEmailContentAggregator:
    from yarndevtools.commands.unittestresultaggregator.common.aggregation import AggregationResults

    def __init__(self, db: UTResultAggregatorDatabase):
        self._db: UTResultAggregatorDatabase = db
        # key: job name, value: Dict[build number, JobBuildData]
        self.fetcher_data_dict: Dict[str, Dict[str, JobBuildData]] = {}
        # key: job name, value: Dict[build number, EmailContent]
        self.aggregator_data_dict: Dict[str, Dict[str, EmailContent]] = {}

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

        if not email_contents:
            raise ValueError(
                "Loaded email contents from DB is empty! Please fill DB by running in execution mode: EMAIL_ONLY"
            )

        # Order is important: We want to digest UT fetcher data first, then UT aggregator data
        entities: List[AggregatorEntity] = build_data + email_contents
        self._process_entities(entities)
        self._do_aggregate(result)

    def _process_entities(self, entities: List[AggregatorEntity]):
        jbd_class_name = JobBuildData.__name__
        ec_class_name = EmailContent.__name__
        dicts = {jbd_class_name: self.fetcher_data_dict, ec_class_name: self.aggregator_data_dict}

        processed: Set[Tuple[str, str]] = set()  # tuple: (job name, build number)
        for e in entities:
            LOG.trace("Processing entity for aggregation: %s", e)
            entity_class = type(e).__name__
            if entity_class not in dicts:
                raise ValueError("Unexpected entity class: {}. Object: {}".format(entity_class, e))

            target_dict = dicts[entity_class]
            builds_per_job = target_dict.setdefault(e.job_name, {})

            key = (e.job_name, e.build_number)
            if key not in processed:
                builds_per_job.setdefault(e.build_number, e)
                processed.add(key)
            else:
                LOG.debug("%s is already processed, not storing build again", key)

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

        for job_name, inner_dict in self.aggregator_data_dict.items():
            for build_number in inner_dict.keys():
                item: EmailContent = self.aggregator_data_dict[job_name][build_number]
                self._process_failed_build(result, FailedBuildAbs.create_from_email(item))
                processed.add((job_name, build_number))

        for job_name, inner_dict in self.fetcher_data_dict.items():
            for build_number in inner_dict.keys():
                key = (job_name, build_number)
                if key not in processed:
                    item: JobBuildData = self.fetcher_data_dict[job_name][build_number]
                    self._process_failed_build(result, FailedBuildAbs.create_from_job_build_data(item))
                else:
                    LOG.debug("%s is already processed during aggregation, not storing build again", key)

        result.finish_processing()
        self._print_stats(result)

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
    # TODO: Make this more abstract and move to pythoncommons?
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

    def _print_stats(self, result: AggregationResults):
        result.print_date_stats()
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

    @staticmethod
    def _process_failed_build(result: AggregationResults, failed_build: FailedBuildAbs):
        LOG.debug("Processing failed build: %s", failed_build.origin())
        result.start_new_context()
        result.match_testcases(failed_build)
        result.finish_context(failed_build)
