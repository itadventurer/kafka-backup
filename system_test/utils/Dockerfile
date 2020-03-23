FROM lensesio/fast-data-dev:latest

ENV BUILD_DEPS bash make gcc g++ cmake curl pkgconfig python perl bsd-compat-headers zlib-dev lz4-dev openssl-dev curl-dev
ENV RUN_DEPS libcurl lz4-libs ca-certificates

RUN echo Installing ; \
  apk add --no-cache --virtual .dev_pkgs $BUILD_DEPS $BUILD_DEPS_EXTRA && \
  apk add --no-cache $RUN_DEPS $RUN_DEPS_EXTRA && \
  echo Building && \
  curl https://codeload.github.com/edenhill/kafkacat/tar.gz/master | tar xzf - && \
  cd kafkacat-* && \
  ./bootstrap.sh && \
  mv kafkacat /usr/bin/ ; \
  echo Cleaning up ; \
  cd / ; \
  rm -rf /usr/src/kafkacat; \
  apk del .dev_pkgs ; \
  rm -rf /var/cache/apk/*

#RUN curl https://codeload.github.com/edenhill/kafkacat/tar.gz/master | tar xzf - && cd kafkacat-* && bash ./bootstrap.sh