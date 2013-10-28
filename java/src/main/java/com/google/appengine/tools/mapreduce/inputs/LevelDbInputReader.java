package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.CorruptDataException;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.impl.util.Crc32c;
import com.google.appengine.tools.mapreduce.impl.util.LevelDbConstants;
import com.google.appengine.tools.mapreduce.impl.util.LevelDbConstants.RecordType;
import com.google.appengine.tools.mapreduce.outputs.LevelDbOutputWriter;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.util.NoSuchElementException;

/**
 * Reads LevelDB formatted input. (Which is produced by {@link LevelDbOutputWriter}
 *
 * If you want to read about the format it is here:
 * {@linkplain "https://code.google.com/p/leveldb/"}
 *
 * Data is read in as needed, so the only state required to reconstruct this class when it is
 * serialized is the offset.
 *
 * In the event that corrupt data is encountered a {@link CorruptDataException} is thrown.
 * If this occurs, do not continue to attempt to read. Behavior is not guaranteed.
 *
 * For internal use only. User code cannot safely depend on this class.
 */
public abstract class LevelDbInputReader extends InputReader<ByteBuffer> {

  private static final long serialVersionUID = -2949371665085068120L;

  private long offset = 0L;

  private final int blockSize;
  /** A temp buffer that is used to hold contents and headers as they are read*/
  private transient ByteBuffer tmpBuffer;
  /** The buffer that contains the data that will ultimately be returned to the caller*/
  private transient ByteBuffer finalRecord;

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
  public void open() {
    offset = 0;
    bytesRead = 0;
    in = createReadableByteChannel();
  }

  /**
   * Calls close on the underlying stream.
   */
  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public void beginSlice() throws IOException {
    tmpBuffer = ByteBuffer.allocate(blockSize);
    tmpBuffer.order(ByteOrder.LITTLE_ENDIAN);
    finalRecord = ByteBuffer.allocate(blockSize);
    finalRecord.order(ByteOrder.LITTLE_ENDIAN);
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
    finalRecord.clear();
    RecordType lastRead = RecordType.NONE;
    while (true) {
      Record record = readPhysicalRecord();
      switch (record.type()) {
        case NONE:
          if (lastRead != RecordType.NONE) {
            throw new CorruptDataException("Found None record following: " + record.type);
          }
          validateRemainderIsEmpty();
          break;
        case FULL:
          if (lastRead != RecordType.NONE) {
            throw new CorruptDataException("Invalid RecordType: " + record.type);
          }
          return record.data().slice();
        case FIRST:
          if (lastRead != RecordType.NONE) {
            throw new CorruptDataException("Invalid RecordType: " + record.type);
          }
          finalRecord = appendToBuffer(finalRecord, record.data());
          break;
        case MIDDLE:
          if (lastRead == RecordType.NONE) {
            throw new CorruptDataException("Invalid RecordType: " + record.type);
          }
          finalRecord = appendToBuffer(finalRecord, record.data());
          break;
        case LAST:
          if (lastRead == RecordType.NONE) {
            throw new CorruptDataException("Invalid RecordType: " + record.type);
          }
          finalRecord = appendToBuffer(finalRecord, record.data());
          finalRecord.flip();
          return finalRecord.slice();
        default:
          throw new CorruptDataException("Invalid RecordType: " + record.type.value());
      }
      lastRead = record.type();
    }
  }

  private void validateRemainderIsEmpty() throws IOException {
    long bytesToBlockEnd = findBytesToBlockEnd();
    tmpBuffer.clear();
    tmpBuffer.limit((int) bytesToBlockEnd);
    int read =  read(tmpBuffer);
    if (read == -1) {
      throw new NoSuchElementException();
    }
    offset += read;
    if (read != bytesToBlockEnd) {
      throw new CorruptDataException(
          "There are " + bytesToBlockEnd + " but " + read + " were read.");
    }
    tmpBuffer.flip();
    for (int i = 0; i < bytesToBlockEnd; i++) {
      byte b = tmpBuffer.get(i);
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
   * @return Record data about the physical record read.
   * @throws IOException
   */
  private Record readPhysicalRecord() throws IOException {
    long bytesToBlockEnd = findBytesToBlockEnd();
    if (bytesToBlockEnd < LevelDbConstants.HEADER_LENGTH) {
      return new Record(RecordType.NONE, null);
    }

    tmpBuffer.clear();
    tmpBuffer.limit(LevelDbConstants.HEADER_LENGTH);
    int bytesRead = read(tmpBuffer);
    if (bytesRead == -1) {
      return new Record(RecordType.NONE, null);
    }
    offset += bytesRead;
    if (bytesRead != LevelDbConstants.HEADER_LENGTH) {
      throw new CorruptDataException("Premature end of file was expecting at least: "
          + bytesToBlockEnd + " but found only: " + bytesRead);
    }
    tmpBuffer.flip();
    int checksum = tmpBuffer.getInt();
    short length = tmpBuffer.getShort();
    RecordType type = RecordType.get(tmpBuffer.get());
    if (length > bytesToBlockEnd || length < 0) {
      throw new CorruptDataException("Length is too large:" + length);
    }

    tmpBuffer.clear();
    tmpBuffer.limit(length);
    bytesRead = read(tmpBuffer);
    offset += bytesRead;
    if (bytesRead != length) {
      throw new CorruptDataException("Premature end of file was expecting at least: " + length
          + " but found only: " + bytesRead);
    }
    if (!isValidCrc(checksum, tmpBuffer, type.value())) {
      throw new CorruptDataException("Checksum doesn't validate.");
    }

    tmpBuffer.flip();
    return new Record(type, tmpBuffer);
  }

  private final long findBytesToBlockEnd() {
    return blockSize - (offset % blockSize);
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
    if (checksum == 0 && type == 0 && data.limit() == 0) {
      return true;
    }
    Crc32c crc = new Crc32c();
    crc.update(type);
    crc.update(data.array(), 0, data.limit());

    return LevelDbConstants.unmaskCrc(checksum) == crc.getValue();
  }

  /**
   * Appends a {@link ByteBuffer} to another. This may modify the inputed buffer that will be
   * appended to.
   *
   * @param to the {@link ByteBuffer} to append to.
   * @param from the {@link ByteBuffer} to append.
   * @return the resulting appended {@link ByteBuffer}
   */
  private static ByteBuffer appendToBuffer(ByteBuffer to, ByteBuffer from) {
    if (to.remaining() < from.remaining()) {
      int capacity = to.capacity();
      while (capacity - to.position() < from.remaining()) {
        capacity *= 2;
      }
      ByteBuffer newBuffer = ByteBuffer.allocate(capacity);
      to.flip();
      newBuffer.put(to);
      to = newBuffer;
      to.order(ByteOrder.LITTLE_ENDIAN);
    }
    to.put(from);
    return to;
  }


}
