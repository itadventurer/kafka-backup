package de.azapps.kafkabackup.common.offset;

public interface OffsetSink {
  void syncConsumerGroups();
  void syncOffsets();
  void flush();
  void close();

}
