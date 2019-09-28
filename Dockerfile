# fyi: this is purely for testing purposes of airtunnel
FROM python:3.6
ENV AIRTUNNEL_HOME=/usr/local/airtunnel
RUN mkdir -p ${AIRTUNNEL_HOME}
ADD . ${AIRTUNNEL_HOME}
WORKDIR ${AIRTUNNEL_HOME}
RUN pip install --no-cache-dir -e ".[dev]"
ENV PYTHONPATH=${AIRTUNNEL_HOME}/src:${AIRTUNNEL_HOME}/test
ENV AIRFLOW_HOME=${AIRTUNNEL_HOME}/test/airflow_home
ENTRYPOINT ["bash", "-c","${AIRTUNNEL_HOME}/test/docker-entrypoint.sh"]