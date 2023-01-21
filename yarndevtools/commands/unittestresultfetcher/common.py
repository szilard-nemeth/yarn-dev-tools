from enum import Enum
from typing import List

CACHED_DATA_DIRNAME = "cached_data"


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
        self.job_names = job_names
