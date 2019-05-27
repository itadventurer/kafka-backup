package de.azapps.kafkabackup.common.segment;

import java.io.*;

/**
 * Format:
 * offset: int64
 * recordFilePosition: int64
 * recordLength: int64
 */
public class SegmentIndexEntry {
	private long offset;
	private long recordFilePosition;
	private long recordByteLength;

	SegmentIndexEntry(long offset, long recordFilePosition, long recordByteLength) {
		this.offset = offset;
		this.recordFilePosition = recordFilePosition;
		this.recordByteLength = recordByteLength;
	}

	static SegmentIndexEntry fromStream(InputStream byteStream) throws IOException {
		DataInputStream stream = new DataInputStream(byteStream);
		long offset = stream.readLong();
		long recordFileOffset = stream.readLong();
		long recordByteLength = stream.readLong();
		return new SegmentIndexEntry(offset, recordFileOffset, recordByteLength);
	}

	public long getOffset() {
		return offset;
	}

	public long recordFilePosition() {
		return recordFilePosition;
	}

	public long recordByteLength() {
		return recordByteLength;
	}

	void writeToStream(OutputStream byteStream) throws IOException {
		DataOutputStream stream = new DataOutputStream(byteStream);
		stream.writeLong(offset);
		stream.writeLong(recordFilePosition);
		stream.writeLong(recordByteLength);
	}

}
