# set base image (host OS)
FROM python:3.8

WORKDIR /home/repos/snemeth/yarn-dev-tools/yarndevtools/cdsw
COPY requirements.txt .
RUN mkdir unit-test-result-fetcher
RUN mkdir branch-comparator
COPY unit-test-result-fetcher unit-test-result-fetcher/
COPY branch-comparator branch-comparator/
RUN pip install -r requirements.txt
RUN apt-get update && apt-get install -y --no-install-recommends \
        rsync

CMD [ "python", "./branch-comparator/cdswsetup.py" ]