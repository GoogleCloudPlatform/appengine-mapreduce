/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.mapreduce;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An iterator iterating over records in an input stream.
 *
 */
class InputStreamIterator implements Iterator<InputStreamIterator.OffsetRecordPair> {
  public static class OffsetRecordPair {
    final private long offset;
    final private byte[] record;

    public OffsetRecordPair(long offset, byte[] record) {
      this.offset = offset;
      this.record = record;
    }

    public long getOffset() {
      return offset;
    }

    public byte[] getRecord() {
      return record;
    }

    public boolean equals(Object rhs) {
      if (!(rhs instanceof OffsetRecordPair)) {
        return false;
      }
      OffsetRecordPair rhsPair = (OffsetRecordPair) rhs;
      return offset == rhsPair.getOffset()
          && Arrays.equals(record, rhsPair.getRecord());
    }

    public int hashCode() {
      return new Long(offset).hashCode() ^ Arrays.hashCode(record);
    }
  }

  private static enum SkipRecordResult {
    AT_EOF, EOF_AFTER_RECORD, TERMINATOR
  }

  private static final Logger log = Logger.getLogger(InputStreamIterator.class.getName());
  private static final int READ_LIMIT = 1024 * 1024;

  private final CountingInputStream input;
  private final long length;
  private final boolean skipFirstTerminator;
  private final byte terminator;

  private OffsetRecordPair currentValue;

  // Note: length may be a negative value when we are reading beyond the split boundary.
  InputStreamIterator(CountingInputStream input, long length,
      boolean skipFirstTerminator, byte terminator) {
    this.input = Preconditions.checkNotNull(input);
    this.length = length;
    this.skipFirstTerminator = skipFirstTerminator;
    this.terminator = terminator;
  }

  // Searches for the start of the next record.
  //
  // Returns
  // TERMINATOR if a terminator is reached. Otherwise,
  // EOF_AFTER_RECORD if EOF is reached after reading some number of non-terminator characters
  // AT_EOF if EOF is reached without any characters being read
  private SkipRecordResult skipUntilNextRecord(InputStream stream) throws IOException {
    boolean readCharSinceTerminator = false;
    int value;
    do {
      value = stream.read();
      if (value == -1) {
        if (readCharSinceTerminator) {
          return SkipRecordResult.EOF_AFTER_RECORD;
        } else {
          return SkipRecordResult.AT_EOF;
        }
      }
      readCharSinceTerminator = true;
    } while (value != (terminator & 0xff));
    return SkipRecordResult.TERMINATOR;
  }

  @Override
  public boolean hasNext() {
    try {
      if (input.getCount() == 0 && skipFirstTerminator) {
        // find the first record start;
        if (skipUntilNextRecord(input) != SkipRecordResult.TERMINATOR) {
          return false;
        }
      }
      // we are reading one record after split-end
      // and are skipping first record for all splits except for the leading one.
      // check if we read one byte ahead of the split.
      if (input.getCount() - 1 >= length) {
        return false;
      }

      long recordStart = input.getCount();
      input.mark(READ_LIMIT);
      SkipRecordResult skipValue = skipUntilNextRecord(input);
      if (skipValue == SkipRecordResult.AT_EOF) {
        return false;
      }

      long recordEnd = input.getCount();
      input.reset();
      int byteValueLen = (int) (recordEnd - recordStart);
      if (skipValue == SkipRecordResult.TERMINATOR) {
        // Skip terminator
        byteValueLen--;
      }
      byte[] byteValue = new byte[byteValueLen];
      ByteStreams.readFully(input, byteValue);
      if (skipValue == SkipRecordResult.TERMINATOR) {
        Preconditions.checkState(1 == input.skip(1)); // skip the terminator
      }
      currentValue = new OffsetRecordPair(recordStart, byteValue);
      return true;
    } catch (IOException e) {
      log.log(Level.WARNING, "Failed to read next record", e);
      return false;
    }
  }

  @Override
  public OffsetRecordPair next() {
    return currentValue;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
