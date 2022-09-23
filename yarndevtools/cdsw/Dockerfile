# set base image (host OS)
FROM python:3.8

WORKDIR /home/cdsw
RUN mkdir job_configs
RUN mkdir scripts
COPY job_configs job_configs/
COPY scripts/install-requirements.sh install-requirements.sh
COPY start_job.py scripts/start_job.py
COPY scripts/clone_downstream_repos.sh scripts/clone_downstream_repos.sh
COPY scripts/clone_upstream_repos.sh scripts/clone-upstream_repos.sh
RUN pip install yarn-dev-tools
RUN apt-get update && apt-get install -y --no-install-recommends \
        rsync

CMD [ "python", "./branch-comparator/cdswsetup.py" ]