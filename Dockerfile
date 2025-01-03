FROM quay.io/astronomer/astro-runtime:12.6.0

# Install dbt into a virtual environment
RUN python -m venv dbt_venv && . dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-core dbt-postgres && deactivate

## set the user as root, helps with the installation permissions :)
USER root

## set environment varibale to avoid ui pop-ups during installations.
ENV DEBIAN_FRONTEND=noninteractive

## install necessary packages in the image,
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    apt-transport-https \
    gnupg2 \
    lsb-release \
    openjdk-17-jdk \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

## if you want to install timezone TZ library for image as well. uncomment below

# RUN apt-get install -y --no-install-recommends \
#  && ln -fs /usr/share/zoneinfo/Asia/Kolkata /etc/localtime \
#  && export DEBIAN_FRONTEND=noninteractive \
#  && apt-get install -y tzdata \
#  && dpkg-reconfigure --frontend noninteractive tzdata \
#  && apt-get autoremove -yqq --purge \
#  && apt-get clean \
#  && rm -rf /var/lib/apt/lists/*


## set up java home. Debian 12 bookworm comes with jdk-17 as default.
# jdk-11 and jdk-8 are unavailable. any attempt to install those will throw errors.
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
RUN export JAVA_HOME