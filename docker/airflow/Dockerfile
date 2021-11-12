ARG env
FROM gcr.io/icentris-ml-${env}/base:latest

RUN pip install --user --upgrade \
        werkzeug==0.16.0 \
        SQLAlchemy==1.3.15 \
        apache-airflow[gcp]==1.10.10 \
        watchgod==0.6

ENV AIRFLOW_HOME=/workspace/airflow \
    AIRFLOW__CORE__LOAD_EXAMPLES=False

RUN airflow initdb

WORKDIR $HOME

COPY --chown=app:users ./docker/airflow/entrypoint bin/entrypoint
COPY --chown=app:users ./docker/airflow/start_airflow bin/start_airflow
COPY --chown=app:users ./docker/airflow/watch_templates bin/watch_templates

RUN chmod +x bin/start_airflow

ENTRYPOINT ["entrypoint"]

EXPOSE 8080
