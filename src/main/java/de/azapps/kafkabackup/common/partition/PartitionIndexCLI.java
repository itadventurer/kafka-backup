package de.azapps.kafkabackup.common.partition;

import org.apache.kafka.common.utils.Exit;

import java.io.IOException;
import java.nio.file.Paths;

public class PartitionIndexCLI {

    public static void main(String[] args) throws IOException, PartitionIndex.IndexException {
        if(args.length != 1) {
            System.out.println("Usage: PartitionIndexCLI partition-index-file");
            Exit.exit(-1);
        }
        String partitionIndexFileName = args[0];
        PartitionIndex partitionIndex = new PartitionIndex(Paths.get(partitionIndexFileName));
        for(PartitionIndexEntry entry : partitionIndex.index()) {
            System.out.println(String.format("File: %s StartOffset: %d", entry.filename(), entry.startOffset()));
        }
    }
}
