# set base image (host OS)
FROM python:3.8

WORKDIR /home/repos/snemeth/yarn-dev-tools/yarndevtools/cdsw
COPY requirements.txt .
RUN mkdir ut-results-reporting
RUN mkdir downstream-branchdiff-reporting
COPY ut-results-reporting ut-results-reporting/
COPY downstream-branchdiff-reporting downstream-branchdiff-reporting/
RUN pip install -r requirements.txt
RUN apt-get update && apt-get install -y --no-install-recommends \
        rsync

CMD [ "python", "./downstream-branchdiff-reporting/cdswsetup.py" ]