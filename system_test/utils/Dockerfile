FROM kafka-backup-dev:latest

RUN apk add --no-cache make gcc g++ cmake curl pkgconfig perl bsd-compat-headers zlib-dev lz4-dev openssl-dev \
 curl-dev libcurl lz4-libs ca-certificates python3 bash python3-dev

# Build librdkafka
RUN mkdir /usr/src && cd /usr/src/ && \
    curl https://codeload.github.com/edenhill/librdkafka/tar.gz/master | tar xzf - && \
    cd librdkafka-master && \
    ./configure && \
    make && make install && \
    cd / && rm -rf /usr/src/

# Install confluent-kafka python

RUN pip3 install confluent-kafka==1.3.0 pykafka==2.8.0dev1
COPY utils.py /usr/bin/utils.py