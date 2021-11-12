ARG env
FROM gcr.io/icentris-ml-${env}/base:latest
USER $UID

RUN pip install --upgrade \
    pycloudsqlproxy \
    google-cloud-bigquery==1.24.0 \
    google-api-python-client==1.9.3 \
    google-cloud-storage==1.26.0 \
    neo4j==4.0.0
