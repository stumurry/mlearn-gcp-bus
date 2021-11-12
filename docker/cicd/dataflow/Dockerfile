ARG env

FROM gcr.io/icentris-ml-${env}/dataflow:latest

# This ARG has to be declared after FROM in order for enviroment to pick it up
ARG env

WORKDIR $WORKSPACE

ENV ENV=${env}

ENV GOOGLE_APPLICATION_CREDENTIALS=/home/app/.config/gcloud/cicd.json

COPY --chown=app:users src .
COPY --chown=app:users docker ./docker

COPY --chown=app:users src/airflow/dags/templates/sql dataflow/tests/sql_templates

WORKDIR $WORKSPACE/dataflow

COPY --chown=app:users docker/cicd/run.sh .

COPY --chown=app:users cicd.json /home/app/.config/gcloud

RUN gcloud config set project icentris-ml-$env && \
    gcloud auth activate-service-account icentris-cicd@icentris-ml-$env.iam.gserviceaccount.com --key-file=/home/app/.config/gcloud/cicd.json

CMD ./run.sh