# Kafka Backup

**Hi all, I am currently not able to maintain this Project on my own. If you are interested in supporting me, please let me know for example by opening an issue.**

Kafka Backup is a tool to back up and restore your Kafka data
including all (configurable) topic data and especially also consumer
group offsets. To the best of our knowledge, Kafka Backup is the only
viable solution to take a cold backup of your Kafka data and restore
it correctly.

It is designed as two connectors for Kafka
Connect: A sink connector (backing data up) and a source connector
(restoring data).

Currently `kafka-backup` supports backup and restore to/from the file
system.

## Features

* Backup and restore topic data
* Backup and restore consumer-group offsets
* Currently supports only backup/restore to/from local file system
* Released as a jar file or packaged as a Docker image

# Getting Started

**Option A) Download binary**

Download the latest release [from GitHub](https://github.com/itadventurer/kafka-backup/releases) and unzip it.

**Option B) Use Docker image**

Pull the latest Docker image from [Docker Hub](https://hub.docker.com/repository/docker/itadventurer/kafka-backup/tags)

**DO NOT USE THE `latest` STAGE IN PRODUCTION**. `latest` are automatic builds of the master branch. Be careful!

**Option C) Build from source**

Just run `./gradlew shadowJar` in the root directory of Kafka Backup. You will find the CLI tools in the `bin` directory.

## Start Kafka Backup

```sh
backup-standalone.sh --bootstrap-server localhost:9092 \
    --target-dir /path/to/backup/dir --topics 'topic1,topic2'
```

In Docker:
```sh
docker run -d -v /path/to/backup-dir/:/kafka-backup/ --rm \
    kafka-backup:[LATEST_TAG] \
    backup-standalone.sh --bootstrap-server kafka:9092 \
    --target-dir /kafka-backup/ --topics 'topic1,topic2'
```

You can pass options via CLI arguments or using environment variables:

| Parameter                                   | Type/required? | Description                                                                                                          |
|---------------------------------------------|----------------|----------------------------------------------------------------------------------------------------------------------|
| `--bootstrap-server`<br/>`BOOTSTRAP_SERVER` | [REQUIRED]     | The Kafka server to connect to                                                                                       |
| `--target-dir`<br/>`TARGET_DIR`             | [REQUIRED]     | Directory where the backup files should be stored                                                                    |
| `--topics`<br/>`TOPICS`                     | <T1,T2,…>      | List of topics to be backed up. You must provide either `--topics` or `--topics-regex`. Not both                     |
| `--topics-regex`<br/>`TOPICS_REGEX`         |                | Regex of topics to be backed up. You must provide either `--topics` or `--topics-regex`. Not both                    |
| `--max-segment-size`<br/>`MAX_SEGMENT_SIZE` |                | Size of the backup segments in bytes DEFAULT: 1GiB                                                                   |
| `--command-config`<br/>`COMMAND_CONFIG`     | <FILE>         | Property file containing configs to be passed to Admin Client. Only useful if you have additional connection options |
| `--debug`<br/>`DEBUG=y`                     |                | Print Debug information                                                                                              |
| `--help`                                    |                | Prints this message                                                                                                  |

**Kafka Backup does not stop!** The Backup process is a continous background job that runs forever as Kafka models data as a stream without end. See [Issue 52: Support point-in-time snapshots](https://github.com/itadventurer/kafka-backup/issues/52) for more information.

## Restore data

```sh
restore-standalone.sh --bootstrap-server localhost:9092 \
    --target-dir /path/to/backup/dir --topics 'topic1,topic2'
```

In Docker:
```sh
docker run -v /path/to/backup/dir:/kafka-backup/ --rm \
    kafka-backup:[LATEST_TAG]
    restore-standalone.sh --bootstrap-server kafka:9092 \
    --source-dir /kafka-backup/ --topics 'topic1,topic2'
```

You can pass options via CLI arguments or using environment variables:


| Parameter                                   | Type/required? | Description                                                                                                                                                                                                                |
|---------------------------------------------|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--bootstrap-server`<br/>`BOOTSTRAP_SERVER` | [REQUIRED]     | The Kafka server to connect to                                                                                                                                                                                             |
| `--source-dir`<br/>`SOURCE_DIR`             | [REQUIRED]     | Directory where the backup files are found                                                                                                                                                                                 |
| `--topics`<br/>`TOPICS`                     | [REQUIRED]     | List of topics to restore                                                                                                                                                                                                  |
| `--batch-size`<br/>`BATCH_SIZE`             |                | Batch size (Default: 1MiB)                                                                                                                                                                                                 |
| `--offset-file`<br/>`OFFSET_FILE`           |                | File where to store offsets. THIS FILE IS CRUCIAL FOR A CORRECT RESTORATION PROCESS IF YOU LOSE IT YOU NEED TO START THE BACKUP FROM SCRATCH. OTHERWISE YOU WILL HAVE DUPLICATE DATA Default: [source-dir]/restore.offsets |
| `--command-config`<br/>`COMMAND_CONFIG`     | <FILE>         | Property file containing configs to be passed to Admin Client. Only useful if you have additional connection options                                                                                                       |
| `--help`<br/>`HELP`                         |                | Prints this message                                                                                                                                                                                                        |
| `--debug`<br/>`DEBUG`                       |                | Print Debug information (if using the environment variable, set it to 'y')                                                                                                                                                 |

## More Documentation

* [FAQ](./docs/FAQ.md)
* [High Level
  Introduction](./docs/Blogposts/2019-06_Introducing_Kafka_Backup.md)
* [Comparing Kafka Backup
  Solutions](./docs/Comparing_Kafka_Backup_Solutions.md)
* [Architecture](./docs/Kafka_Backup_Architecture.md)
* [Tooling](./docs/Tooling.md)

## License

This project is licensed under the Apache License Version 2.0 (see
[LICENSE](./LICENSE)).
