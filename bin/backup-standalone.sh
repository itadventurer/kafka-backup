#!/bin/bash
# saner programming env: these switches turn some bugs into errors
set -o errexit -o pipefail -o noclobber -o nounset

# -allow a command to fail with !’s side effect on errexit
# -use return value from ${PIPESTATUS[0]}, because ! hosed $?
! getopt --test >/dev/null
if [[ ${PIPESTATUS[0]} -ne 4 ]]; then
  echo '`getopt --test` failed in this environment.'
  exit 1
fi

WORKING_DIR="$(mktemp -d -t kafka-backup.XXXXXX)"

# Cleanup after SIGTERM/SIGINT
_term() {
  rm -r "$WORKING_DIR"
}

trap _term SIGTERM
trap _term INT

##################################### Parse arguments

OPTIONS="h"
LONGOPTS=bootstrap-server:,target-dir:,topics:,topics-regex:,max-segment-size:,command-config:,help,debug,snapshot

HELP=$(
  cat <<END
--bootstrap-server  [REQUIRED] The Kafka server to connect to
--target-dir        [REQUIRED] Directory where the backup files should be stored
--topics            <T1,T2,…>  List of topics to be backed up. You must provide either --topics or --topics-regex. Not both
--topics-regex                 Regex of topics to be backed up. You must provide either --topics or --topics-regex. Not both
--max-segment-size             Size of the backup segments in bytes DEFAULT: 1GiB
--command-config    <FILE>     Property file containing configs to be
                               passed to Admin Client. Only useful if you have additional connection options
--help                         Prints this message
--debug                        Print Debug information (if using the environment variable, set it to 'y')
--snapshot                     One-off backup mode

You can also set all parameters using environment variables. Use CAPITAL LETTERS and underscores (_) instead of dashes (-).
E.g. BOOTSTRAP_SERVER instead of --bootstrap-server
END
)

# Allow to set parameters using environment variables

[ -z ${BOOTSTRAP_SERVER+x} ] && BOOTSTRAP_SERVER=""
[ -z ${TARGET_DIR+x} ] && TARGET_DIR=""
[ -z ${TOPICS+x} ] && TOPICS=""
[ -z ${TOPICS_REGEX+x} ] && TOPICS_REGEX=""
[ -z ${MAX_SEGMENT_SIZE+x} ] && MAX_SEGMENT_SIZE="$((1 * 1024 * 1024 * 1024))" # 1GiB
[ -z ${COMMAND_CONFIG+x} ] && COMMAND_CONFIG=""
[ -z ${DEBUG+x} ] && DEBUG="n"
[ -z ${SNAPSHOT+x} ] && SNAPSHOT="false"

PLUGIN_PATH="$(dirname "${BASH_SOURCE[0]}")"
CONNECT_BIN=""

# -temporarily store output to be able to check for errors
# -activate quoting/enhanced mode (e.g. by writing out “--options”)
# -pass arguments only via   -- "$@"   to separate them correctly
! PARSED=$(getopt --options=$OPTIONS --longoptions=$LONGOPTS --name "$0" -- "$@")
if [[ ${PIPESTATUS[0]} -ne 0 ]]; then
  # e.g. return value is 1
  #  then getopt has complained about wrong arguments to stdout
  exit 2
fi
# read getopt’s output this way to handle the quoting right:
eval set -- "$PARSED"

while true; do
  case "$1" in
  --bootstrap-server)
    BOOTSTRAP_SERVER="$2"
    shift 2
    ;;
  --target-dir)
    TARGET_DIR="$2"
    shift 2
    ;;
  --topics)
    TOPICS="$2"
    shift 2
    ;;
  --topics-regex)
    TOPICS_REGEX="$2"
    shift 2
    ;;
  --max-segment-size)
    MAX_SEGMENT_SIZE="$2"
    shift 2
    ;;
  --command-config)
    COMMAND_CONFIG="$2"
    shift 2
    ;;
  -h | --help)
    echo "$HELP"
    exit 0
    ;;
  -d | --debug)
    DEBUG=y
    shift
    ;;
  -s | --snapshot)
    SNAPSHOT=true
    shift
    ;;
  --)
    shift
    break
    ;;
  *)
    echo "$1 $2 Programming error"
    exit 3
    ;;
  esac
done

##################################### Check arguments

if [ -z "$BOOTSTRAP_SERVER" ]; then
  echo "--bootstrap-server is missing"
  echo "$HELP"
  exit 1
fi

if [ -z "$TARGET_DIR" ]; then
  echo "--target-dir is missing"
  echo "$HELP"
  exit 1
fi

if [ ! -d "$TARGET_DIR" ]; then
  mkdir "$TARGET_DIR"
fi

if { [ -z "$TOPICS" ] && [ -z "$TOPICS_REGEX" ]; } || { [ -n "$TOPICS" ] && [ -n "$TOPICS_REGEX" ]; }; then
  echo "You need to provide either --topics or --topics-regex not both nor none"
  echo "$HELP"
  exit 1
fi

if [ -n "$COMMAND_CONFIG" ] && [ ! -f "$COMMAND_CONFIG" ]; then
  echo "no such file $COMMAND_CONFIG"
  exit 1
fi

if [ ! -f "$PLUGIN_PATH/kafka-backup.jar" ]; then
  echo "Cannot find the kafka-backup.jar in $PLUGIN_PATH. Please set --backup-jar accordingly"
  exit 1
fi

if [ -n "$(command -v connect-standalone.sh)" ]; then
  CONNECT_BIN="connect-standalone.sh"
fi

if [ -n "$(command -v connect-standalone)" ]; then
  CONNECT_BIN="connect-standalone"
fi

if [ -z "$CONNECT_BIN" ]; then
  echo "Cannot find connect-standalone or connect-standalone.sh in PATH. please add it"
  exit 1
fi

##################################### Create configs

# Standalone Worker Config

WORKER_CONFIG="$WORKING_DIR/standalone.properties"

cat <<EOF >"$WORKER_CONFIG"
bootstrap.servers=$BOOTSTRAP_SERVER
key.converter=org.apache.kafka.connect.converters.ByteArrayConverter
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
header.converter=org.apache.kafka.connect.converters.ByteArrayConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
offset.storage.file.filename=$WORKING_DIR/kafka-backup.offsets
offset.flush.interval.ms=10000
plugin.path=$PLUGIN_PATH
EOF

if [ -n "$COMMAND_CONFIG" ]; then
  cat "$COMMAND_CONFIG" >>"$WORKER_CONFIG"
fi

# Connector config

CONNECTOR_CONFIG="$WORKING_DIR/connector.properties"

cat <<EOF >"$CONNECTOR_CONFIG"
name=backup-sink
connector.class=de.azapps.kafkabackup.sink.BackupSinkConnector
tasks.max=1
key.converter=org.apache.kafka.connect.converters.ByteArrayConverter
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
header.converter=org.apache.kafka.connect.converters.ByteArrayConverter
target.dir=$TARGET_DIR
max.segment.size.bytes=$MAX_SEGMENT_SIZE
cluster.bootstrap.servers=$BOOTSTRAP_SERVER
snapshot=$SNAPSHOT
EOF

if [ -n "$TOPICS" ]; then
  echo "topics:$TOPICS" >>"$CONNECTOR_CONFIG"
fi

if [ -n "$TOPICS_REGEX" ]; then
  echo "topics.regex:$TOPICS_REGEX" >>"$CONNECTOR_CONFIG"
fi

if [ -n "$COMMAND_CONFIG" ]; then
  sed 's/^/cluster./' "$COMMAND_CONFIG" >>"$CONNECTOR_CONFIG"
fi

LOG4J_CONFIG="$WORKING_DIR/log4j.properties"

if [ "$DEBUG" == "y" ]; then
  echo "$WORKER_CONFIG:"
  sed 's/^/> /' "$WORKER_CONFIG"
  echo
  echo "$CONNECTOR_CONFIG:"
  sed 's/^/> /' "$CONNECTOR_CONFIG"
  echo
  cat <<EOF >"$LOG4J_CONFIG"
log4j.rootLogger=INFO, stdout
log4j.logger.de.azapps.kafkabackup=DEBUG
log4j.logger.org.apache.zookeeper=ERROR
log4j.logger.org.I0Itec.zkclient=ERROR
log4j.logger.org.reflections=ERROR
log4j.logger.org.glassfish=ERROR

log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=[%d] %p %m (%c)%n
EOF
else
  cat <<EOF >"$LOG4J_CONFIG"
log4j.rootLogger=WARN, stdout
log4j.logger.de.azapps.kafkabackup=INFO
log4j.logger.org.apache.zookeeper=ERROR
log4j.logger.org.I0Itec.zkclient=ERROR
log4j.logger.org.reflections=ERROR
log4j.logger.org.glassfish=ERROR

log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=[%d] %p %m (%c)%n
EOF
fi

##################################### Start Connect Standalone

KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:${LOG4J_CONFIG}" "$CONNECT_BIN" "$WORKER_CONFIG" "$CONNECTOR_CONFIG"
