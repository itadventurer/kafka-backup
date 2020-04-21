# How to restore to a different topic

> I didn't see in the documentation if it's possible to be able to restore to a different destination topic, such as mybackupedtopic-restored. It would help with testing restore procedures without disturbing the existing topic, among other things.

Simply rename the topic directories in the Backup target.

# Restoring a multi-partition topic does not work

> When I restore topic with 24 partitions it creates topic with one partitions and restore failed.
> Restore successful if I create 24 partitions topic before restore. 

You need to create the topic manually before restore. For a "real" backup scenario you also need to backup and restore Zookeeper

# Error "Plugin class loader for connector was not found" 

```sh
ERROR Plugin class loader for connector: 'de.azapps.kafkabackup.sink.BackupSinkConnector' was not found. Returning: org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader@5b068087 (org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader:165)
```

You forgot to build the jar file. Either get an official release of Kafka Backup or run `./gradlew shadowJar` in the root directory of Kafka Backup.