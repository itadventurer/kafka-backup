package de.azapps.kafkabackup.common.segment;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SegmentIndex {
	private List<SegmentIndexEntry> index = new ArrayList<>();
	private long lastValidRecordOffset = -1;
	private long lastValidIndexPosition = 0;
	private FileOutputStream fileOutputStream;
	private FileInputStream fileInputStream;

	SegmentIndex(File file) throws IOException, IndexException {
		this.fileInputStream = new FileInputStream(file);
		this.fileOutputStream = new FileOutputStream(file, true);
		fileInputStream.getChannel().position(0);
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

	int size() {
		return index.size();
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
