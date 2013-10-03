package com.google.appengine.tools.mapreduce.impl.util;

import com.google.appengine.tools.mapreduce.inputs.LevelDbInputReader;
import com.google.appengine.tools.mapreduce.outputs.LevelDbOutputWriter;

/**
 * A set of constants needed by {@link LevelDbInputReader} and {@link LevelDbOutputWriter}.
 */
public final class LevelDbConstants {


  /**
   * The size of the blocks records are written in.
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
   * An enumerated type that describes the type of the record being stored.
   * These byte values must match those of the leveldb log format for
   * compatibility between different implementations
   * of the {@link LevelDbOutputWriter}.
   *
   */
  public enum RecordType {
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
    return ((((crc >> 15) | (crc << 17)) + LevelDbConstants.CRC_MASK_DELTA) & 0xFFFFFFFFL);
  }

  /**
   * Unmasks the crc.
   *
   * @param maskedCrc a masked crc.
   * @return an unmasked crc.
   */
  public static long unmaskCrc(long maskedCrc) {
    long rot = (maskedCrc - CRC_MASK_DELTA) & 0xFFFFFFFFL;
    return (((rot >> 17) | (rot << 15)) & 0xFFFFFFFFL);
  }
}
