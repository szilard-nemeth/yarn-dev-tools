import logging
from collections import UserDict
from typing import Dict

from yarndevtools.commands.unittestresultfetcher.model import JenkinsJobResult
from yarndevtools.common.common_model import JobBuildData
from yarndevtools.common.db import Database, MongoDbConfig, JenkinsBuildDatabase

LOG = logging.getLogger(__name__)


class JenkinsJobResults(UserDict):
    def __init__(self, job_results):
        super().__init__()
        self.data: Dict[str, JenkinsJobResult] = job_results  # key: Jenkins job name
        self._index = 0

    def __getitem__(self, job_name):
        return self.data[job_name]

    def __setitem__(self, job_name, job_result):
        self.data[job_name] = job_result

    def __delitem__(self, job_name):
        del self.data[job_name]

    def __len__(self):
        return len(self.data)

    def print_email_status(self):
        LOG.info("Printing email send status for jobs and builds...")
        for job_name, job_result in self.data.items():
            for job_url, job_build_data in job_result._builds_by_url.items():
                LOG.info("Job URL: %s, email sent: %s", job_url, job_build_data.mail_sent)

    def get_by_job_and_url(self, job_name, url):
        return self.data[job_name]._builds_by_url[url]


class UTResultFetcherDatabase(Database):
    def __init__(self, conf: MongoDbConfig):
        super().__init__(conf)
        self._jenkins_build_data_db = JenkinsBuildDatabase(conf)

    def has_build_data(self, build_url):
        return self._jenkins_build_data_db.has_build_data(build_url)

    def save_build_data(self, build_data: JobBuildData):
        self._jenkins_build_data_db.save_build_data(build_data)

    def save_job_results(self, job_results: JenkinsJobResults, log: bool = False):
        LOG.info("Saving Jenkins job results to Database")
        if log:
            LOG.debug("Final job results object: %s", job_results)

        job_build_datas = []
        for job_result in job_results.data.values():
            for build in job_result.builds:
                job_build_datas.append(build)
        self._jenkins_build_data_db.save_all_build_data(job_build_datas)

    def load_job_results(self) -> JenkinsJobResults:
        LOG.info("Trying to load Jenkins job results from Database")
        job_build_data_objs = self._jenkins_build_data_db.find_and_validate_all_build_data()

        job_results_dict = {}
        # TODO dummy values passed
        for build_data in job_build_data_objs:
            if build_data.job_name not in job_results_dict:
                job_results_dict[build_data.job_name] = JenkinsJobResult.create_empty(-1, -1)
            job_results_dict[build_data.job_name].add_build(build_data)

        for job_result in job_results_dict.values():
            job_result.finalize()

        job_results = JenkinsJobResults(job_results_dict)
        job_results.print_email_status()
        LOG.info("Loaded Jenkins job results from Database. Length of collection: %d", len(job_results))
        return job_results
