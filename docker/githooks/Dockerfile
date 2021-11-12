ARG env
FROM gcr.io/icentris-ml-${env}/base:latest

USER root

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git

USER $UID

COPY --chown=$UID:$GID ./docker/githooks/symlink ./docker/githooks/linter $WORKSPACE/

RUN chmod +x $WORKSPACE/symlink && \
    chmod +x $WORKSPACE/linter

CMD [$WORKSPACE, '/symlink']