// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.api.files.Crc32c;
import com.google.appengine.api.files.FileReadChannel;
import com.google.appengine.api.files.RecordReadChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Logger;

  /**
   * An enumerated type that describes the type of the record being stored.
   * These byte values must match those of the leveldb log format for
   * compatibility between different implementations
   * of the {@link RecordWriteChannelImpl}.
   *
   */
enum RecordType {
    NONE((byte) 0x00),
    FULL((byte) 0x01),
    FIRST((byte) 0x02),
    MIDDLE((byte) 0x03),
    LAST((byte) 0x04),
    UNKNOWN((byte) 0xFF);

    private byte value;

    private RecordType(byte value) {
      this.value = value;
    }
    /**
     * The byte value of the record type is written to the file as part of the
     * header.
     * @return the byte value of the record type.
     */
    public byte value() {
      return this.value;
    }

    /**
     * Converts a byte value into a {@link RecordType} enum.
     * @param value the byte value of the {@link RecordType} you want.
     * @return a {@link RecordType} that corresponds to the inputed byte value.
     */
    public static RecordType get(byte value) {
      switch(value) {
        case 0x00: return NONE;
        case 0x01: return FULL;
        case 0x02: return FIRST;
        case 0x03: return MIDDLE;
        case 0x04: return LAST;
        default: return UNKNOWN;
      }
    }
}

/**
 * A set of constants needed by {@link RecordReadChannelImpl} and {@link RecordWriteChannelImpl}.
 *
 */
final class RecordConstants {

  /**
   * Size of a block.
   */
  public static final int BLOCK_SIZE = 32 * 1024;

  /**
   * Header length in data.
   */
  public static final int HEADER_LENGTH = 7;

  /**
   *  CRC Mask. Comes from http://leveldb.googlecode.com/svn/trunk/util/crc32c.h
   */
  public static final int CRC_MASK_DELTA = 0xa282ead8;



  /**
   * Masks the crc.
   *
   * Motivation taken from leveldb:
   *    it is problematic to compute the CRC of a string that
   *    contains embedded CRCs.  Therefore we recommend that CRCs stored
   *    somewhere (e.g., in files) should be masked before being stored.
   * @param crc the crc to mask.
   * @return the masked crc.
   */
  public static long maskCrc(long crc) {
    return ((((crc >> 15) | (crc << 17)) + RecordConstants.CRC_MASK_DELTA) & 0xFFFFFFFFL);
  }

  /**
   * Unmasks the crc.
   * @param maskedCrc a masked crc.
   * @return an unmasked crc.
   */
  public static long unmaskCrc(long maskedCrc) {
    long rot = (maskedCrc - CRC_MASK_DELTA) & 0xFFFFFFFFL;
    return (((rot >> 17) | (rot << 15)) & 0xFFFFFFFFL);
 }
}

// TODO(ohler): Make RecordReadChannelImpl less spammy and delete this.
public final class NonSpammingRecordReadChannel implements RecordReadChannel {
  private Logger log = Logger.getLogger(NonSpammingRecordReadChannel.class.getName());

  @SuppressWarnings("serial")
  static final class RecordReadException extends Exception {
    public RecordReadException(String errorMessage) {
      super(errorMessage);
    }
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

  private final FileReadChannel input;
  // ByteBuffers for reading in the file. Instance variables to
  // prevent having to reallocate memory.
  private ByteBuffer blockBuffer;
  private ByteBuffer finalRecord;

  /**
   * @param input a {@link FileReadChannel} that holds Records to read from.
   */
  public NonSpammingRecordReadChannel(FileReadChannel input) {
    this.input = input;
    blockBuffer = ByteBuffer.allocate(RecordConstants.BLOCK_SIZE);
    blockBuffer.order(ByteOrder.LITTLE_ENDIAN);
    finalRecord = ByteBuffer.allocate(RecordConstants.BLOCK_SIZE);
    finalRecord.order(ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ByteBuffer readRecord() throws IOException {
    finalRecord.clear();
    RecordType lastRead = RecordType.NONE;
    while (true) {
      try {
        Record record = readPhysicalRecord();
        if (record == null) {
          return null;
        }
        switch (record.type()) {
          case NONE:
            sync();
            break;
          case FULL:
            if (lastRead != RecordType.NONE) {
              throw new RecordReadException("Invalid RecordType: "
                + record.type);
            }
            return record.data().slice();
          case FIRST:
            if (lastRead != RecordType.NONE) {
              throw new RecordReadException("Invalid RecordType: "
                  + record.type);
            }
            finalRecord = appendToBuffer(finalRecord, record.data());
            break;
          case MIDDLE:
            if (lastRead == RecordType.NONE) {
              throw new RecordReadException("Invalid RecordType: "
                + record.type);
            }
            finalRecord = appendToBuffer(finalRecord, record.data());
            break;
          case LAST:
            if (lastRead == RecordType.NONE) {
              throw new RecordReadException("Invalid RecordType: "
                + record.type);
            }
            finalRecord = appendToBuffer(finalRecord, record.data());
            finalRecord.flip();
            return finalRecord.slice();
          default:
            throw new RecordReadException("Invalid RecordType: " + record.type.value());
        }
        lastRead = record.type();
      } catch (RecordReadException e) {
        //log.warning(input + " position " + input.position() + "; " + e.getMessage());
        finalRecord.clear();
        sync();
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long position() throws IOException {
    return input.position();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void position(long newPosition) throws IOException {
    input.position(newPosition);
  }

  /**
   * Reads the next record from the RecordIO data stream.
   *
   * @return Record data about the physical record read.
   * @throws IOException
   */
  private Record readPhysicalRecord()
          throws IOException, RecordReadException {
    int bytesToBlockEnd = (int) (RecordConstants.BLOCK_SIZE -
        (input.position() % RecordConstants.BLOCK_SIZE));

    if (bytesToBlockEnd < RecordConstants.HEADER_LENGTH) {
      return new Record(RecordType.NONE, null);
    }

    blockBuffer.clear();
    blockBuffer.limit(RecordConstants.HEADER_LENGTH);
    int bytesRead = input.read(blockBuffer);
    if (bytesRead != RecordConstants.HEADER_LENGTH) {
      return null;
    }
    blockBuffer.flip();
    int checksum = blockBuffer.getInt();
    short length = blockBuffer.getShort();
    RecordType type = RecordType.get(blockBuffer.get());
    if (length > bytesToBlockEnd || length < 0) {
      throw new RecordReadException("Length is too large:" + length);
    }

    blockBuffer.clear();
    blockBuffer.limit(length);
    bytesRead = input.read(blockBuffer);
    if (bytesRead != length) {
      return null;
    }
    if (!isValidCrc(checksum, blockBuffer, type.value())) {
      throw new RecordReadException("Checksum doesn't validate.");
    }

    blockBuffer.flip();
    return new Record(type, blockBuffer);
  }


  /**
   * Moves to the start of the next block.
   * @throws IOException
   */
  private void sync() throws IOException {
    long padLength = RecordConstants.BLOCK_SIZE -
        (input.position() % RecordConstants.BLOCK_SIZE);
    input.position(input.position() + padLength);
  }

  /**
   * Validates that the {@link Crc32c} validates.
   * @param checksum the checksum in the record.
   * @param data the {@link ByteBuffer} of the data in the record.
   * @param type the byte representing the {@link RecordType} of the record.
   * @return true if the {@link Crc32c} validates.
   */
  private static boolean isValidCrc(int checksum, ByteBuffer data, byte type) {
    Crc32c crc = new Crc32c();
    crc.update(type);
    crc.update(data.array(), 0, data.limit());

    return RecordConstants.unmaskCrc(checksum) == crc.getValue();
  }

  /**
   * Appends a {@link ByteBuffer} to another. This may modify
   * the inputed buffer that will be appended to.
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
