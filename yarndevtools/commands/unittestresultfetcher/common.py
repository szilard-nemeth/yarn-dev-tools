from enum import Enum
from typing import List

CACHED_DATA_DIRNAME = "cached_data"


class JobNameUtils:
    @staticmethod
    def escape_job_name(job_name: str):
        # TODO yarndevtoolsv2: Check call hierarchy of this --> Think about unified solution
        return job_name.replace(".", "_")


class UnitTestResultFetcherMode(Enum):
    __job_names_by_mode__ = {}

    JENKINS_MASTER = (
        "jenkins_master",
        "https://master-02.jenkins.cloudera.com/",
        [
            "cdpd-master-Hadoop-Common-Unit",
            "cdpd-master-Hadoop-HDFS-Unit",
            "cdpd-master-Hadoop-MR-Unit",
            "cdpd-master-Hadoop-YARN-Unit",
            "CDH-7.1-maint-Hadoop-Common-Unit",
            "CDH-7.1-maint-Hadoop-HDFS-Unit",
            "CDH-7.1-maint-Hadoop-MR-Unit",
            "CDH-7.1-maint-Hadoop-YARN-Unit",
            "CDH-7.1.7.1000-Hadoop-Common-Unit",
            "CDH-7.1.7.1000-Hadoop-HDFS-Unit",
            "CDH-7.1.7.1000-Hadoop-MR-Unit",
            "CDH-7.1.7.1000-Hadoop-YARN-Unit",
        ],
    )
    MAWO = ("MAWO", "http://build.infra.cloudera.com/", ["Mawo-UT-hadoop-CDPD-7.x", "Mawo-UT-hadoop-CDPD-7.1.x"])

    def __init__(self, mode_name: str, jenkins_base_url: str, job_names: List[str]):
        self.mode_name = mode_name
        self.jenkins_base_url = jenkins_base_url
        self.job_names = [JobNameUtils.escape_job_name(jn) for jn in job_names]

    @staticmethod
    def get_mode_by_job_name(job_name_param):
        if not UnitTestResultFetcherMode.__job_names_by_mode__:
            d = {}
            for m in UnitTestResultFetcherMode:
                for job_name in m.job_names:
                    d[job_name] = m
            UnitTestResultFetcherMode.__job_names_by_mode__ = d

        d = UnitTestResultFetcherMode.__job_names_by_mode__
        escaped_job_name = JobNameUtils.escape_job_name(job_name_param)
        found = escaped_job_name in d

        if not found:
            raise ValueError(
                "Unrecognized job name (original): {}. \n"
                "Escaped job name: {}\n"
                "Known job names: {}".format(job_name_param, escaped_job_name, d.keys())
            )
        if found:
            return d[escaped_job_name]
