package de.azapps.kafkabackup.common.segment;

import de.azapps.kafkabackup.common.partition.PartitionIndex;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SegmentIndex {
	private Path indexFile;
	private List<SegmentIndexEntry> index = new ArrayList<>();
	private long lastValidRecordOffset = -1;
	private long lastValidIndexPosition = 1; // mind the magic byte!
	private FileOutputStream fileOutputStream;
	private FileInputStream fileInputStream;
    private static final byte V1_MAGIC_BYTE = 0x01;

	public SegmentIndex(Path indexFile) throws IOException, IndexException {
		this.indexFile = indexFile;
		initFile();
		while (true) {
			try {
				SegmentIndexEntry segmentIndexEntry = SegmentIndexEntry.fromStream(fileInputStream);
				if (segmentIndexEntry.getOffset() <= lastValidRecordOffset) {
					throw new IndexException("Offsets must be always increasing! There is something terribly wrong in your index!");
				}
				index.add(segmentIndexEntry);
				lastValidRecordOffset = segmentIndexEntry.getOffset();
				lastValidIndexPosition = fileInputStream.getChannel().position();
			} catch (EOFException e) {
				// reached End of File
				break;
			}
		}
	}

	private void initFile() throws IOException, IndexException {
		if (!Files.isRegularFile(indexFile)) {
			Files.createFile(indexFile);
			fileOutputStream = new FileOutputStream(indexFile.toFile());
			fileOutputStream.write(V1_MAGIC_BYTE);
		} else {
			fileOutputStream = new FileOutputStream(indexFile.toFile(), true);
		}
		this.fileInputStream = new FileInputStream(indexFile.toFile());
		byte[] v1Validation = new byte[1];
		if(fileInputStream.read(v1Validation) != 1 || v1Validation[0] != V1_MAGIC_BYTE) {
			throw new IndexException("Cannot validate Magic Byte in the beginning of the index " + indexFile);
		}
	}

	void addEntry(SegmentIndexEntry segmentIndexEntry) throws IOException, IndexException {
		if (segmentIndexEntry.getOffset() <= lastValidRecordOffset) {
			throw new IndexException("Offsets must be always increasing! There is something terribly wrong in your index!");
		}
		fileOutputStream.getChannel().position(lastValidIndexPosition);
		segmentIndexEntry.writeToStream(fileOutputStream);
		lastValidIndexPosition = fileOutputStream.getChannel().position();
		lastValidRecordOffset = segmentIndexEntry.getOffset();
		index.add(segmentIndexEntry);
	}

	Optional<SegmentIndexEntry> lastIndexEntry() {
		if (!index.isEmpty()) {
			return Optional.of(index.get(index.size() - 1));
		} else {
			return Optional.empty();
		}
	}

	long lastValidStartPosition() {
		if (!index.isEmpty()) {
			return index.get(index.size() - 1).recordFilePosition();
		} else {
			return 0L;
		}

	}

	Optional<SegmentIndexEntry> getByPosition(int position) {
		if(position >= index.size()) {
			return Optional.empty();
		} else {
			return Optional.of(index.get(position));
		}
	}

	Optional<Long> findByOffset(long offset) {
		for(SegmentIndexEntry current : index) {
			if(current.getOffset() == offset) {
				return Optional.of(current.recordFilePosition());
			}
		}
		return Optional.empty();
	}

	Optional<Long> findEarliestWithHigherOrEqualOffset(long offset) {
		for(SegmentIndexEntry current : index) {
			if(current.getOffset() >= offset) {
				return Optional.of(current.recordFilePosition());
			}
		}
		return Optional.empty();
	}

	int size() {
		return index.size();
	}

	public List<SegmentIndexEntry> index() {
		return index;
	}

	void flush() throws IOException {
		fileOutputStream.flush();
	}

	void close() throws IOException {
		fileInputStream.close();
		fileOutputStream.close();
	}

	public static class IndexException extends Exception {
		IndexException(String message) {
			super(message);
		}
	}

}
