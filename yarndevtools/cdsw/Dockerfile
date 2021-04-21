# set base image (host OS)
FROM python:3.8

WORKDIR /home/repos/snemeth/yarn-dev-tools/yarndevtools/cdsw
COPY requirements.txt .
RUN mkdir daily-ut-results-reporting
RUN mkdir daily-downstream-branchdiff-reporting
COPY daily-ut-results-reporting daily-ut-results-reporting/
COPY daily-downstream-branchdiff-reporting daily-downstream-branchdiff-reporting/
RUN pip install -r requirements.txt
RUN apt-get update && apt-get install -y --no-install-recommends \
        rsync

CMD [ "python", "./daily-downstream-branchdiff-reporting/cdswsetup.py" ]