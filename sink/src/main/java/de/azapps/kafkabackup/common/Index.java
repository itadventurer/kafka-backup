package de.azapps.kafkabackup.common;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Index {
	private List<IndexEntry> index = new ArrayList<>();
	private long lastValidRecordOffset = -1;
	private long lastValidIndexPosition = 0;
	private FileOutputStream fileOutputStream;
	private FileInputStream fileInputStream;

	public Index(File file) throws IOException, IndexException {
		this.fileInputStream = new FileInputStream(file);
		this.fileOutputStream = new FileOutputStream(file, true);
		fileInputStream.getChannel().position(0);
		while (true) {
			try {
				IndexEntry indexEntry = IndexEntry.fromStream(fileInputStream);
				if (indexEntry.getOffset() <= lastValidRecordOffset) {
					throw new IndexException("Offsets must be always increasing! There is something terribly wrong in your index!");
				}
				index.add(indexEntry);
				lastValidRecordOffset = indexEntry.getOffset();
				lastValidIndexPosition = fileInputStream.getChannel().position();
			} catch (EOFException e) {
				// reached End of File
				break;
			}
		}
	}

	public void addEntry(IndexEntry indexEntry) throws IOException, IndexException {
		if (indexEntry.getOffset() <= lastValidRecordOffset) {
			throw new IndexException("Offsets must be always increasing! There is something terribly wrong in your index!");
		}
		fileOutputStream.getChannel().position(lastValidIndexPosition);
		indexEntry.writeToStream(fileOutputStream);
		lastValidIndexPosition = fileOutputStream.getChannel().position();
		lastValidRecordOffset = indexEntry.getOffset();
		index.add(indexEntry);
	}

	public Optional<IndexEntry> lastIndexEntry() {
		if (!index.isEmpty()) {
			return Optional.of(index.get(index.size() - 1));
		} else {
			return Optional.empty();
		}
	}

	Optional<IndexEntry> getByPosition(int position) {
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
