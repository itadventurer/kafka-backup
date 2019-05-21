package de.azapps.kafkabackup.common.segment;

import java.io.*;

/**
 * Format:
 * offset: int64
 * recordFileOffset: int64
 * recordLength: int64
 */
public class SegmentIndexEntry {
	private long offset;
	private long recordFileOffset;
	private long recordByteLength;

	public SegmentIndexEntry(long offset, long recordFileOffset, long recordByteLength) {
		this.offset = offset;
		this.recordFileOffset = recordFileOffset;
		this.recordByteLength = recordByteLength;
	}

	public static SegmentIndexEntry fromStream(InputStream byteStream) throws IOException {
		DataInputStream stream = new DataInputStream(byteStream);
		long offset = stream.readLong();
		long recordFileOffset = stream.readLong();
		long recordByteLength = stream.readLong();
		return new SegmentIndexEntry(offset, recordFileOffset, recordByteLength);
	}

	public long getOffset() {
		return offset;
	}

	public long getRecordFileOffset() {
		return recordFileOffset;
	}

	public long getRecordByteLength() {
		return recordByteLength;
	}

	public void writeToStream(OutputStream byteStream) throws IOException {
		DataOutputStream stream = new DataOutputStream(byteStream);
		stream.writeLong(offset);
		stream.writeLong(recordFileOffset);
		stream.writeLong(recordByteLength);
	}

}
