#!/bin/bash
# saner programming env: these switches turn some bugs into errors
set -o errexit -o pipefail -o noclobber -o nounset

# -allow a command to fail with !’s side effect on errexit
# -use return value from ${PIPESTATUS[0]}, because ! hosed $?
! getopt --test >/dev/null
if [[ ${PIPESTATUS[0]} -ne 4 ]]; then
  # shellcheck disable=SC2016
  echo '`getopt --test` failed in this environment.'
  exit 1
fi

WORKING_DIR="$(mktemp -d -t kafka-backup.XXXXXX)"

# Cleanup after SIGTERM/SIGINT
_term() {
  echo "Detected finished restore. Terminating Kafka Connect…"
  kill $PID
  echo "Waiting for Kafka Connect to terminate…"
  sleep 5
  kill $PID2
  rm -r "$WORKING_DIR"
  echo ""
}

trap _term SIGTERM
trap _term INT

##################################### Parse arguments

OPTIONS="h"
LONGOPTS=bootstrap-server:,source-dir:,topics:,batch-size:,offset-file:,command-config:,help,debug

HELP=$(
  cat <<END
--bootstrap-server  [REQUIRED] The Kafka server to connect to
--source-dir        [REQUIRED] Directory where the backup files are found
--topics            [REQUIRED] List of topics to restore
--batch-size                   Batch size (Default: 1MiB)
--offset-file                  File where to store offsets. THIS FILE IS CRUCIAL FOR A CORRECT RESTORATION PROCESS
                               IF YOU LOSE IT YOU NEED TO START THE BACKUP FROM SCRATCH. OTHERWISE YOU WILL HAVE DUPLICATE DATA
                               Default: [source-dir]/restore.offsets
--command-config    <FILE>     Property file containing configs to be
                               passed to Admin Client. Only useful if you have additional connection options
--help                         Prints this message
--debug                        Print Debug information (if using the environment variable, set it to 'y')

You can also set all parameters using environment variables. Use CAPITAL LETTERS and underscores (_) instead of dashes (-).
E.g. BOOTSTRAP_SERVER instead of --bootstrap-server
END
)

[ -z ${BOOTSTRAP_SERVER+x} ] && BOOTSTRAP_SERVER=""
[ -z ${SOURCE_DIR+x} ] && SOURCE_DIR=""
[ -z ${TOPICS+x} ] && TOPICS=""
[ -z ${OFFSET_FILE+x} ] && OFFSET_FILE=""
[ -z ${BATCH_SIZE+x} ] && BATCH_SIZE="$((1 * 1024 * 1024))"
[ -z ${COMMAND_CONFIG+x} ] && COMMAND_CONFIG=""
[ -z ${DEBUG+x} ] && DEBUG="n"

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
  --source-dir)
    SOURCE_DIR="$2"
    shift 2
    ;;
  --topics)
    TOPICS="$2"
    shift 2
    ;;
  --batch-size)
    BATCH_SIZE="$2"
    shift 2
    ;;
  --offset-file)
    OFFSET_FILE="$2"
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

if [ -z "$SOURCE_DIR" ]; then
  echo "--source-dir is missing"
  echo "$HELP"
  exit 1
fi

if [ ! -d "$SOURCE_DIR" ]; then
  echo "Directory $SOURCE_DIR does not exist."
  exit 1
fi

if [ -z "$TOPICS" ]; then
  echo "--topics is missing"
  echo "$HELP"
  exit 1
fi

if [ -z "$OFFSET_FILE" ]; then
  OFFSET_FILE="$SOURCE_DIR/restore.offsets"
fi

if ! touch "$OFFSET_FILE"; then
  echo "cannot touch $OFFSET_FILE. Please make sure it is writable"
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
offset.storage.file.filename=$OFFSET_FILE
offset.flush.interval.ms=10000
plugin.path=$PLUGIN_PATH
EOF

if [ -n "$COMMAND_CONFIG" ]; then
  cat "$COMMAND_CONFIG" >>"$WORKER_CONFIG"
fi

# Connector config

CONNECTOR_CONFIG="$WORKING_DIR/connector.properties"

cat <<EOF >"$CONNECTOR_CONFIG"
name=backup-source
connector.class=de.azapps.kafkabackup.source.BackupSourceConnector
tasks.max=1
topics=$TOPICS
key.converter=org.apache.kafka.connect.converters.ByteArrayConverter
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
header.converter=org.apache.kafka.connect.converters.ByteArrayConverter
source.dir=$SOURCE_DIR
batch.size=$BATCH_SIZE
cluster.bootstrap.servers=$BOOTSTRAP_SERVER
cluster.key.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer
cluster.value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer
EOF

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

# We need to wait for Kafka Connect to print the `All records read`
# line. The user should also see the log messages. Therefore we start
# Kafka Connect in the background and write the output to a temp
# file. We `tail` the output to stdout in the background (but the user
# can see it). In the foreground we grep the temp file for the
# expected line and kill all background processes when we encounter it

# Start Kafka connect in the background
KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:${LOG4J_CONFIG}" "$CONNECT_BIN" "$WORKER_CONFIG" "$CONNECTOR_CONFIG" >>"$WORKING_DIR/log" &
PID=$!
# Print the content of the log to stdout. Fork it to the background
tail -F "$WORKING_DIR/log" &
PID2=$!
# We need to wait some time (5 seconds should be fine) until something is written to the log – otherwise weird things happen
sleep 5
# Wait for the finish message
grep -q "All records read. Restore was successful" <(tail -F "$WORKING_DIR/log" 2>/dev/null)
# Terminate both background processes and cleanup
_term
