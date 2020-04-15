#!/bin/bash
java -cp "$( dirname "${BASH_SOURCE[0]}" )/kafka-backup.jar" de.azapps.kafkabackup.cli.PartitionIndexCLI "$@"