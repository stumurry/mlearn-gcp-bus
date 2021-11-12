# NOTE: The argument env has to be lowercase or just change the name since is a reserved word
ARG env

FROM gcr.io/icentris-ml-${env}/ml-apis:latest

# This ARG has to be declared after FROM in order for enviroment to pick it up because eveything is reset after FROM
ARG env

WORKDIR $WORKSPACE

ARG env

ENV ENV=${env}

ENV GOOGLE_APPLICATION_CREDENTIALS=/home/app/.config/gcloud/cicd.json

COPY --chown=app:users src .
COPY --chown=app:users docker ./docker

WORKDIR $WORKSPACE/apis

COPY --chown=app:users docker/cicd/run.sh .

COPY --chown=app:users cicd.json /home/app/.config/gcloud

RUN gcloud config set project icentris-ml-$env && \
    gcloud auth activate-service-account icentris-cicd@icentris-ml-$env.iam.gserviceaccount.com --key-file=/home/app/.config/gcloud/cicd.json

# CMD ./run.sh