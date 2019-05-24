package de.azapps.kafkabackup.common.segment;

import org.apache.kafka.common.utils.Exit;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class SegmentIndexCLI {
    public static void main(String[] args) throws IOException, SegmentIndex.IndexException {
        if (args.length != 1) {
            System.out.println("Usage: SegmentIndexCLI Segment-index-file");
            Exit.exit(-1);
        }
        String segmentIndexFileName = args[0];
        if(!segmentIndexFileName.endsWith("_index")) {
            segmentIndexFileName += "_index";
        }
        SegmentIndex segmentIndex = new SegmentIndex(new File(segmentIndexFileName));
        List<SegmentIndexEntry> index = segmentIndex.index();
        long previousOffset = index.get(0).getOffset()-1;
        for (SegmentIndexEntry entry : index) {
            System.out.print(String.format("Offset: %d Position: %d Length: %d", entry.getOffset(), entry.recordFilePosition(), entry.recordByteLength()));
            if(entry.getOffset() > previousOffset + 1) {
                System.out.print(" <- FYI Here is a gap");
            }
            System.out.println();
            previousOffset = entry.getOffset();
        }
        System.out.println(String.format("%d entries in Index", index.size()));
    }
}
