FROM quay.io/astronomer/astro-runtime:6.0.3
# Install OpenJDK-8
USER root

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
WORKDIR /app
COPY requirements.txt ./
RUN apt update -y && apt-get install -y software-properties-common && \
    apt-add-repository 'deb http://security.debian.org/debian-security stretch/updates main' && apt update -y && \
    apt-get install -y openjdk-8-jdk-headless && \
    pip install --no-cache-dir -r requirements.txt && \
    export JAVA_HOME && \
    apt-get clean

USER 1001
