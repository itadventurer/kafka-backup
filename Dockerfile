# Build Kafka Backup
FROM gradle:6.3.0-jdk8 AS builder
WORKDIR /opt/kafka-backup
COPY . /opt/kafka-backup
RUN gradle --no-daemon check test shadowJar

# Build Docker Image with Kafka Backup Jar
FROM openjdk:8u212-jre-alpine

ARG kafka_version=2.5.0
ARG scala_version=2.12
ARG glibc_version=2.31-r0

ENV KAFKA_VERSION=$kafka_version \
    SCALA_VERSION=$scala_version \
    KAFKA_HOME=/opt/kafka \
    GLIBC_VERSION=$glibc_version

ENV PATH=${PATH}:${KAFKA_HOME}/bin

RUN apk add --no-cache bash curl \
 && wget "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -O "/tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
 && tar xfz /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt \
 && rm /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
 && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} ${KAFKA_HOME} \
 && wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/${GLIBC_VERSION}/glibc-${GLIBC_VERSION}.apk \
 && apk add --no-cache --allow-untrusted glibc-${GLIBC_VERSION}.apk \
 && rm glibc-${GLIBC_VERSION}.apk

COPY ./bin /opt/kafka-backup/
COPY --from=builder /opt/kafka-backup/build/libs/kafka-backup.jar /opt/kafka-backup/

ENV PATH="${KAFKA_HOME}/bin:/opt/kafka-backup/:${PATH}"
