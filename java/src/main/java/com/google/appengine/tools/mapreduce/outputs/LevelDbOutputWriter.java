package com.google.appengine.tools.mapreduce.outputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.util.Crc32c;
import com.google.appengine.tools.mapreduce.impl.util.LevelDbConstants;
import com.google.appengine.tools.mapreduce.impl.util.LevelDbConstants.RecordType;
import com.google.appengine.tools.mapreduce.inputs.LevelDbInputReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * An implementation of a LevelDb format writer. 
 * Output of this class can be read by {@link LevelDbInputReader}.
 * 
 * If you want to read about the format spec it is here: 
 * {@linkplain "https://code.google.com/p/leveldb/"}
 */
public class LevelDbOutputWriter extends ForwardingOutputWriter<ByteBuffer> {

  private static final long serialVersionUID = -2081903739320744064L;

  /**
   * A class that holds information needed to write a physical record.
   */
  private static final class Record {
    private final RecordType type;
    private final int bytes;

    private Record() {
      type = RecordType.NONE;
      bytes = 0;
    }

    private Record(RecordType type, int bytes) {
      Preconditions.checkArgument(type != RecordType.UNKNOWN);
      Preconditions.checkArgument(bytes >= 0);
      this.type = type;
      this.bytes = bytes;
    }

    /**
     * Returns the number of bytes that needs to be written.
     * 
     * @return the number of bytes.
     */
    int getBytes() {
      return bytes;
    }

    /**
     * Returns the type of record that needs to be written.
     * 
     * @return the type.
     */
    RecordType getType() {
      return type;
    }

  }

  private transient ByteBuffer writeBuffer;
  private OutputWriter<ByteBuffer> delegate;
  private int blockSize;
  private int numBlocksWritten = 0;

  public LevelDbOutputWriter(OutputWriter<ByteBuffer> delegate) {
    this(delegate, LevelDbConstants.BLOCK_SIZE);
  }
  
  @VisibleForTesting
  LevelDbOutputWriter(OutputWriter<ByteBuffer> delegate, int blockSize) {
    Preconditions.checkArgument(blockSize > 0);
    this.delegate = checkNotNull(delegate);
    this.blockSize = blockSize;
  }

  @Override
  public void beginSlice() throws IOException {
    writeBuffer = ByteBuffer.allocate(blockSize);
    writeBuffer.order(ByteOrder.LITTLE_ENDIAN);
    super.beginSlice();
  }

  @Override
  public void endSlice() throws IOException {
    // Pad the block so that the underlying file is always complete blocks
    padAndWriteBlock(false);
    super.endSlice();
  }
  

  @Override
  public void write(ByteBuffer data) throws IOException {
    Record lastRecord = new Record();

    // do loop used to ensure the loop executes at least once to handle the 0-byte data record
    do {
      // Get the next data record
      int bytesToBlockEnd = writeBuffer.remaining();
      Record currentRecord = createRecord(data, bytesToBlockEnd, lastRecord);
      writePhysicalRecord(data, currentRecord);
      lastRecord = currentRecord;

      // If the remaining space in block is too small for header, pad the rest of the block
      bytesToBlockEnd = writeBuffer.remaining();
      if ((bytesToBlockEnd < LevelDbConstants.HEADER_LENGTH) && (bytesToBlockEnd > 0)) {
        writeBlanksToBuffer(bytesToBlockEnd);
        bytesToBlockEnd = 0;
      }

      // If the block is complete, write to underlying channel
      if (bytesToBlockEnd == 0) {
        writeBuffer.flip();
        delegate.write(writeBuffer);
        writeBuffer.clear();
        numBlocksWritten++;
      }
    } while (data.hasRemaining());
  }

  /**
   * Closes the OutputChannel.
   */
  @Override
  public void close() throws IOException {
    if (writeBuffer != null) {
      writeBuffer.clear();
    }
    super.close();
  }

  /**
   * Fills a {@link Record} object with data about the physical record to write.
   * 
   * @param data the users data.
   * @param bytesToBlockEnd remaining bytes in the current block.
   * @param lastRecord a {@link Record} representing the last physical record written.
   * @return the {@link Record} with new write data.
   **/
  private static Record createRecord(ByteBuffer data, int bytesToBlockEnd, Record lastRecord) {
    int bytesToDataEnd = data.remaining();
    RecordType type = RecordType.UNKNOWN;
    int bytes = -1;
    if ((lastRecord.getType() == RecordType.NONE)
        && ((bytesToDataEnd + LevelDbConstants.HEADER_LENGTH) <= bytesToBlockEnd)) {
      // Can write entire record in current block
      type = RecordType.FULL;
      bytes = bytesToDataEnd;
    } else if (lastRecord.getType() == RecordType.NONE) {
      // On first write but can't fit in current block
      type = RecordType.FIRST;
      bytes = bytesToBlockEnd - LevelDbConstants.HEADER_LENGTH;
    } else if (bytesToDataEnd + LevelDbConstants.HEADER_LENGTH <= bytesToBlockEnd) {
      // At end of record
      type = RecordType.LAST;
      bytes = bytesToDataEnd;
    } else {
      // In middle somewhere
      type = RecordType.MIDDLE;
      bytes = bytesToBlockEnd - LevelDbConstants.HEADER_LENGTH;
    }
    return new Record(type, bytes);
  }

  /**
   * This method creates a record inside of a {@link ByteBuffer}
   * 
   * @param data The data to output.
   * @param record A {@link Record} object that describes
   *        which data to write.
   */
  private void writePhysicalRecord(ByteBuffer data, Record record) {
    writeBuffer.putInt(generateCrc(data.array(), data.position(), record.getBytes(),
        record.getType()));
    writeBuffer.putShort((short) record.getBytes());
    writeBuffer.put(record.getType().value());
    int oldLimit = data.limit();
    data.limit(data.position() + record.getBytes());
    writeBuffer.put(data);
    data.limit(oldLimit);
  }


  /**
   * Fills the {@link ByteBuffer} with 0x00;
   * 
   * @param numBlanks the number of bytes to pad.
   */
  private void writeBlanksToBuffer(int numBlanks) {
    for (int i = 0; i < numBlanks; i++) {
      writeBuffer.put((byte) 0x00);
    }
  }

  /**
   * Generates a CRC32C checksum using {@link Crc32c} for a specific record.
   * 
   * @param data The user data over which the checksum will be generated.
   * @param off The offset into the user data at which to begin the computation.
   * @param len The length of user data to use in the computation.
   * @param type The {@link RecordType} of the record, which is included in the
   *        checksum.
   * @return the masked checksum.
   */
  private int generateCrc(byte[] data, int off, int len, RecordType type) {
    Crc32c crc = new Crc32c();
    crc.update(type.value());
    crc.update(data, off, len);
    return (int) LevelDbConstants.maskCrc(crc.getValue());
  }

  /**
   * Adds padding to the stream until to the end of the block.
   *
   * @throws IOException
   */
  void padAndWriteBlock(boolean writeEmptyBlockIfFull) throws IOException {
    int remaining = writeBuffer.remaining();
    if (writeEmptyBlockIfFull || remaining < blockSize) {
      writeBlanksToBuffer(remaining);
      writeBuffer.flip();
      delegate.write(writeBuffer);
      writeBuffer.clear();
      numBlocksWritten++;
    }
  }
  
  @Override
  protected OutputWriter<ByteBuffer> getDelegate() {
    return delegate;
  }
  
  protected int getNumBlocksWritten() {
    return numBlocksWritten;
  }

}
