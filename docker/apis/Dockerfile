ARG env

FROM gcr.io/icentris-ml-${env}/base:latest

# This ARG has to be declared after FROM in order for enviroment to pick it up because eveything is reset after FROM
ARG env=local

WORKDIR $HOME

COPY --chown=app:users ./docker/apis/start_apis bin/start_apis
COPY --chown=app:users ./src/apis $WORKSPACE/apis

RUN chmod +x bin/start_apis

WORKDIR $WORKSPACE/apis

ENV ENV=${env}

RUN pip install -e . --user

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 0 server:app
