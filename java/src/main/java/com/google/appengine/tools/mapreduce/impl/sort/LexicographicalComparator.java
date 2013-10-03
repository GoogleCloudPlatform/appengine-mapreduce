package com.google.appengine.tools.mapreduce.impl.sort;

import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedLongs;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * Acts as a comparator for two byte buffers. This class is used to sort keys lexicographically.
 * Meaning that if there are two strings using the same character encoding they should sort
 * properly. Also if positive or unsigned integers or longs are encoded to a fixed width these
 * should sort correctly also.
 *
 */
public final class LexicographicalComparator implements Comparator<ByteBuffer> {

  @Override
  public int compare(ByteBuffer left, ByteBuffer right) {
    if (left == right) {
      return 0;
    }
    return compare(left, left.position(), left.remaining(), right, right.position(), right.limit());
  }

  static int compare(ByteBuffer a, int aPos, int aLen, ByteBuffer b, int bPos, int bLen) {
    int minLength = Math.min(aLen, bLen);
    int minWords = minLength / Longs.BYTES;

    for (int i = 0; i < minWords; i++) {
      int offset = i * Longs.BYTES;
      int result = UnsignedLongs.compare(a.getLong(aPos + offset), b.getLong(bPos + offset));
      if (result != 0) {
        return result;
      }
    }
    // The epilogue to cover the last (minLength % 8) bytes.
    for (int i = minWords * Longs.BYTES; i < minLength; i++) {
      int result = UnsignedBytes.compare(a.get(aPos + i), b.get(bPos + i));
      if (result != 0) {
        return result;
      }
    }
    return aLen - bLen;
  }

}