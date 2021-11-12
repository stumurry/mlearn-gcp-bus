ARG env
FROM python:3.7.7-stretch
ARG env=local
ARG PROJECT_ID=icentris-ml-${env}

ARG USER="app"
ARG UID="1000"
ARG GID="100"
ARG CLOUD_SDK_VERSION=232.0.0

ENV USER=$USER \
    UID=$UID \
    GID=$GID

ENV CLOUD_SDK_VERSION=$CLOUD_SDK_VERSION

USER root

RUN apt-get -qqy update && apt-get install -qqy \
        curl \
        gcc \
        python-dev \
        python-setuptools \
        apt-transport-https \
        lsb-release \
        openssh-client \
        git \
        gnupg \
        openssl \
        bash \
        less \
        vim \
    && CLOUD_SDK_REPO="cloud-sdk-$(grep VERSION_CODENAME /etc/os-release | cut -d '=' -f 2)" &&\
    echo "deb https://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list &&\
    echo "deb https://apt.kubernetes.io/ kubernetes-stretch main" | tee -a /etc/apt/sources.list.d/kubernetes.list &&\
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -; \
    apt-get update && apt-get install -y \
        kubectl \
        google-cloud-sdk \
        google-cloud-sdk-app-engine-python \
        google-cloud-sdk-app-engine-python-extras \
        google-cloud-sdk-datalab \
        google-cloud-sdk-datastore-emulator \
        google-cloud-sdk-pubsub-emulator \
        google-cloud-sdk-cloud-build-local \
        kubectl && \
    gcloud --version && \
    kubectl version --client

RUN useradd -m -s /bin/bash -g $GID -u $UID $USER && \
    chmod g+w /etc/passwd

ENV WORKSPACE=/workspace \
    HOME=/home/$USER
RUN mkdir $WORKSPACE && \
    chown -R $UID:$GID $WORKSPACE && \
    chmod ug+s $WORKSPACE && \
    mkdir /home/$USER/bin && \
    chown -R $UID:$GID /home/$USER/bin && \
    chmod ug+s /home/$USER/bin

USER $UID

ENV PATH=/home/$USER/bin:/home/$USER/.local/bin:$PATH

#RUN echo $WORKSPACE && echo $HOME && echo $PATH

RUN gcloud config set project $PROJECT_ID

RUN pip install \
        crcmod==1.7 \
        pytest==5.4.3 \
        pytest-cov==2.10.0 \
        Faker==4.1.1 \
        flake8==3.8.3 \
        dockerfile==3.1.0 \
        bandit==1.6.2

WORKDIR $WORKSPACE
COPY --chown=app:users src/jeeves jeeves
COPY --chown=app:users src/shared shared
WORKDIR $WORKSPACE/jeeves
RUN pip install -e . --user
WORKDIR $WORKSPACE
#RUN rm -r $WORKSPACE/jeeves && rm -r $WORKSPACE/shared
