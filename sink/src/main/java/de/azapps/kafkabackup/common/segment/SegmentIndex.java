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

	public SegmentIndex(File file) throws IOException, IndexException {
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

	public void addEntry(SegmentIndexEntry segmentIndexEntry) throws IOException, IndexException {
		if (segmentIndexEntry.getOffset() <= lastValidRecordOffset) {
			throw new IndexException("Offsets must be always increasing! There is something terribly wrong in your index!");
		}
		fileOutputStream.getChannel().position(lastValidIndexPosition);
		segmentIndexEntry.writeToStream(fileOutputStream);
		lastValidIndexPosition = fileOutputStream.getChannel().position();
		lastValidRecordOffset = segmentIndexEntry.getOffset();
		index.add(segmentIndexEntry);
	}

	public Optional<SegmentIndexEntry> lastIndexEntry() {
		if (!index.isEmpty()) {
			return Optional.of(index.get(index.size() - 1));
		} else {
			return Optional.empty();
		}
	}

	Optional<SegmentIndexEntry> getByPosition(int position) {
		if(position >= index.size()) {
			return Optional.empty();
		} else {
			return Optional.of(index.get(position));
		}
	}

	int size() {
		return index.size();
	}

	public void flush() throws IOException {
		fileOutputStream.flush();
	}

	public void close() throws IOException {
		fileInputStream.close();
		fileOutputStream.close();
	}

	public static class IndexException extends Exception {
		IndexException(String message) {
			super(message);
		}
	}

}
