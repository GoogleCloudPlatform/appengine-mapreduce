package com.google.appengine.tools.mapreduce.inputs;

import static com.google.appengine.tools.mapreduce.impl.util.LevelDbConstants.HEADER_LENGTH;

import com.google.appengine.tools.mapreduce.CorruptDataException;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.impl.util.Crc32c;
import com.google.appengine.tools.mapreduce.impl.util.LevelDbConstants;
import com.google.appengine.tools.mapreduce.impl.util.LevelDbConstants.RecordType;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Reads LevelDB formatted input. (Which is produced by
 * {@link com.google.appengine.tools.mapreduce.outputs.LevelDbOutputWriter})
 *
 * If you want to read about the format it is here:
 * {@linkplain "https://code.google.com/p/leveldb/"}
 *
 * This implementation deviates from the specification above, in that it allows blocks to be zero
 * padded regardless of how much data is in the block, rather than only if the block is within 6
 * bytes of full.
 *
 * Data is read in as needed, so the only state required to reconstruct this class when it is
 * serialized is the offset.
 *
 * In the event that corrupt data is encountered a {@link CorruptDataException} is thrown. If this
 * occurs, do not continue to attempt to read. Behavior is not guaranteed.
 *
 * For internal use only. User code cannot safely depend on this class.
 */
public abstract class LevelDbInputReader extends InputReader<ByteBuffer> {

  private static final long serialVersionUID = -2949371665085068120L;

  private long offset = 0L;

  private final int blockSize;
  /** A temp buffer that is used to hold contents and headers as they are read*/
  private transient ByteBuffer tmpBuffer;

  private long bytesRead;
  private ReadableByteChannel in;

  public LevelDbInputReader() {
    this(LevelDbConstants.BLOCK_SIZE);
  }

  @VisibleForTesting
  protected LevelDbInputReader(int blockSize) {
    super();
    this.blockSize = blockSize;
  }

  /**
   * @return A Serializable ReadableByteChannel from which data may be read.
   */
  public abstract ReadableByteChannel createReadableByteChannel();

  private int read(ByteBuffer result) throws IOException {
    int totalRead = 0;
    while (result.hasRemaining()) {
      int read = in.read(result);
      if (read == -1) {
        if (totalRead == 0) {
          totalRead = -1;
        }
        break;
      } else {
        totalRead += read;
        bytesRead += read;
      }
    }
    return totalRead;
  }

  protected long getBytesRead() {
    return bytesRead;
  }

  @Override
  public void beginShard() {
    offset = 0;
    bytesRead = 0;
    in = createReadableByteChannel();
  }

  /**
   * Calls close on the underlying stream.
   */
  @Override
  public void endShard() throws IOException {
    in.close();
  }

  @Override
  public void beginSlice() {
    tmpBuffer = allocate(blockSize);
  }

  private static ByteBuffer allocate(int size) {
    ByteBuffer result = ByteBuffer.allocate(size);
    result.order(ByteOrder.LITTLE_ENDIAN);
    return result;
  }

  private static final class Record {
    private final ByteBuffer data;
    private final RecordType type;

    public Record(RecordType type, ByteBuffer data) {
      this.type = type;
      this.data = data;
    }

    public ByteBuffer data() {
      return this.data;
    }

    public RecordType type() {
      return this.type;
    }
  }

  /**
   * @return How far into the file has been read.
   */
  long getOffset() {
    return offset;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ByteBuffer next() throws IOException, NoSuchElementException {
    Record record = readPhysicalRecord(true);
    while (record.type().equals(RecordType.NONE)) {
      validateBufferIsZeros(record.data());
      record = readPhysicalRecord(true);
    }
    if (record.type().equals(RecordType.FULL)) {
      return record.data();
    }
    if (record.type().equals(RecordType.FIRST)) {
      ArrayList<ByteBuffer> result = new ArrayList<>();
      result.add(record.data());
      record = readPhysicalRecord(false);
      while (record.type().equals(RecordType.MIDDLE)) {
        result.add(record.data());
        record = readPhysicalRecord(false);
      }
      if (!record.type().equals(RecordType.LAST)) {
        throw new CorruptDataException("Unterminated first block. Found: " + record.type.value());
      }
      result.add(record.data());
      return copyAll(result);
    }
    throw new CorruptDataException("Unexpected RecordType: " + record.type.value());
  }

  private void validateBufferIsZeros(ByteBuffer buffer) {
    for (int i = buffer.position(); i < buffer.limit(); i++) {
      byte b = buffer.get(i);
      if (b != 0) {
        throw new CorruptDataException("Found a non-zero byte: " + b
            + " before the end of the block " + i
            + " bytes after encountering a RecordType of NONE");
      }
    }
  }

  /**
   * Reads the next record from the LevelDb data stream.
   *
   * @param expectEnd if end of stream encountered will throw {@link NoSuchElementException} when
   *        true and {@link CorruptDataException} when false.
   * @return Record data of the physical record read.
   * @throws IOException
   */
  private Record readPhysicalRecord(boolean expectEnd) throws IOException {
    int bytesToBlockEnd = findBytesToBlockEnd();
    if (bytesToBlockEnd < HEADER_LENGTH) {
      readToTmp(bytesToBlockEnd, expectEnd);
      return createRecordFromTmp(RecordType.NONE);
    }
    readToTmp(HEADER_LENGTH, expectEnd);

    int checksum = tmpBuffer.getInt();
    int length = tmpBuffer.getShort();
    if (length > bytesToBlockEnd || length < 0) {
      throw new CorruptDataException("Length is too large:" + length);
    }
    RecordType type = RecordType.get(tmpBuffer.get());
    if (type == RecordType.NONE && length == 0) {
      length = bytesToBlockEnd - HEADER_LENGTH;
    }
    readToTmp(length, false);

    if (!isValidCrc(checksum, tmpBuffer, type.value())) {
      throw new CorruptDataException("Checksum doesn't validate.");
    }
    return createRecordFromTmp(type);
  }

  /**
   * Reads {@code length} number of bytes into {@link #tmpBuffer}. {@link #offset} is incremented
   * and {@link #tmpBuffer} is flipped.
   *
   * @param expectEnd if end of stream encountered will throw {@link NoSuchElementException} when
   *        true and {@link CorruptDataException} when false.
   */
  private void readToTmp(int length, boolean expectEnd) throws IOException {
    tmpBuffer.clear();
    tmpBuffer.limit(length);
    int read = read(tmpBuffer);
    if (read == -1 && expectEnd) {
      throw new NoSuchElementException();
    }
    if (read != length) {
      throw new CorruptDataException("Premature end of file was expecting at least: "
          + length + " but found only: " + read);
    }
    offset += read;
    tmpBuffer.flip();
  }

  private Record createRecordFromTmp(RecordType type) {
    ByteBuffer data = allocate(tmpBuffer.remaining());
    data.put(tmpBuffer);
    data.flip();
    return new Record(type, data);
  }

  private final int findBytesToBlockEnd() {
    return (int) (blockSize - (offset % blockSize));
  }

  /**
   * Validates that the {@link Crc32c} validates.
   *
   * @param checksum the checksum in the record.
   * @param data the {@link ByteBuffer} of the data in the record.
   * @param type the byte representing the {@link RecordType} of the record.
   * @return true if the {@link Crc32c} validates.
   */
  private static boolean isValidCrc(int checksum, ByteBuffer data, byte type) {
    if (checksum == 0 && type == 0) {
      return true;
    }
    Crc32c crc = new Crc32c();
    crc.update(type);
    crc.update(data.array(), 0, data.limit());

    return LevelDbConstants.unmaskCrc(checksum) == crc.getValue();
  }

  /**
   * Copies a list of byteBuffers into a single new byteBuffer.
   */
  private static ByteBuffer copyAll(List<ByteBuffer> buffers) {
    int size = 0;
    for (ByteBuffer b : buffers) {
      size += b.remaining();
    }
    ByteBuffer result = allocate(size);
    for (ByteBuffer b : buffers) {
      result.put(b);
    }
    result.flip();
    return result;
  }
}
