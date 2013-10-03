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
    left.mark();
    right.mark();
    try {
      return comp(left, right);
    } finally {
      left.reset();
      right.reset();
    }
  }

  private int comp(ByteBuffer left, ByteBuffer right) {
    int result = 0;
    int minLength = Math.min(left.remaining(), right.remaining());
    int minWords = minLength / Longs.BYTES;

    for (int i = 0; i < minWords; i++) {
      result = UnsignedLongs.compare(left.getLong(), right.getLong());
      if (result != 0) {
        return result;
      }
    }
    // The epilogue to cover the last (minLength % 8) bytes.
    for (int i = minWords * Longs.BYTES; i < minLength; i++) {
      result = UnsignedBytes.compare(left.get(), right.get());
      if (result != 0) {
        return result;
      }
    }
    return left.remaining() - right.remaining();
  }

}