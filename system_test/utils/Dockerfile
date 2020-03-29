FROM confluentinc/cp-kafka:5.4.1

ENV BUILD_DEPS build-essential zlib1g-dev liblz4-dev libssl-dev libsasl2-dev cmake libcurl4-openssl-dev pkg-config
ENV RUN_DEPS libssl1.0 libsasl2-2 ca-certificates curl

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends $BUILD_DEPS $RUN_DEPS && \
    echo "Building" && \
    curl https://codeload.github.com/edenhill/kafkacat/tar.gz/master | tar xzf - && \
    cd kafkacat-* && \
    ./bootstrap.sh && \
    mv kafkacat /usr/bin/ && \
    echo "Cleaning up" && \
    cd / && \
    rm -rf /usr/src/kafkacat&& \
    apt-get purge -y --auto-remove $BUILD_DEPS && \
    apt-get clean -y && \
    apt-get autoclean -y && \
    rm /var/log/dpkg.log /var/log/alternatives.log /var/log/apt/*.log&& \
    rm -rf /var/lib/apt/lists/*