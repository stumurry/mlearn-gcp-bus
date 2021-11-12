ARG env
FROM gcr.io/icentris-ml-${env}/base:latest

RUN pip install --upgrade \
        apache-beam[gcp]==2.24.0

COPY --chown=app:users ./src/dataflow $WORKSPACE/dataflow
WORKDIR $WORKSPACE/dataflow
RUN pip install -e . --user
RUN rm -r $WORKSPACE/dataflow
