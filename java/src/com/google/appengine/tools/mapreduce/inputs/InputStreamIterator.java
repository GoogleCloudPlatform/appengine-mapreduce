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

package com.google.appengine.tools.mapreduce.inputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An iterator iterating over records in an input stream.
 *
 */
class InputStreamIterator extends AbstractIterator<InputStreamIterator.OffsetRecordPair> {

  private static final Logger log = Logger.getLogger(InputStreamIterator.class.getName());
  private static final int READ_LIMIT = 1024 * 1024;

  private final CountingInputStream input;
  private final long length;
  private final boolean skipFirstTerminator;
  private final byte terminator;

  // Note: length may be a negative value when we are reading beyond the split boundary.
  InputStreamIterator(CountingInputStream input, long length, boolean skipFirstTerminator, byte terminator) {
    this.input = checkNotNull(input, "Null input");
    this.length = length;
    this.skipFirstTerminator = skipFirstTerminator;
    this.terminator = terminator;
  }

  @Override
  public OffsetRecordPair computeNext() {
    try {
      if (input.getCount() == 0 && skipFirstTerminator) {
        // find the first record start;
        if (skipUntilNextRecord(input) != SkipRecordResult.TERMINATOR) {
          return endOfData();
        }
      }
      // we are reading one record after split-end
      // and are skipping first record for all splits except for the leading one.
      // check if we read one byte ahead of the split.
      if (input.getCount() - 1 >= length) {
        return endOfData();
      }

      long recordStart = input.getCount();
      input.mark(READ_LIMIT);
      SkipRecordResult skipValue = skipUntilNextRecord(input);
      if (skipValue == SkipRecordResult.AT_EOF) {
        return endOfData();
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
        Preconditions.checkState(input.skip(1) == 1); // skip the terminator
      }
      return new OffsetRecordPair(recordStart, byteValue);
    } catch (IOException e) {
      log.log(Level.WARNING, "Failed to read next record", e);
      return endOfData();
    }
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
        return readCharSinceTerminator ? SkipRecordResult.EOF_AFTER_RECORD
            : SkipRecordResult.AT_EOF;
      }
      readCharSinceTerminator = true;
    } while (value != (terminator & 0xff));
    return SkipRecordResult.TERMINATOR;
  }

  private enum SkipRecordResult {
    AT_EOF, EOF_AFTER_RECORD, TERMINATOR
  }

  public static final class OffsetRecordPair {
    private final long offset;
    private final byte[] record;

    public OffsetRecordPair(long offset, byte[] record) {
      this.offset = offset;
      this.record = checkNotNull(record, "Null record");
    }

    public long getOffset() {
      return offset;
    }

    public byte[] getRecord() {
      return record;
    }

    @Override public String toString() {
      return getClass().getSimpleName() + "("
          + offset + ", "
          + Arrays.toString(record)
          + ")";
    }

    @Override public final boolean equals(Object o) {
      if (o == this) { return true; }
      if (!(o instanceof OffsetRecordPair)) { return false; }
      OffsetRecordPair other = (OffsetRecordPair) o;
      return offset == other.offset
          && Arrays.equals(record, other.record);
    }

    @Override public final int hashCode() {
      return Objects.hashCode(offset, Arrays.hashCode(record));
    }
  }

}
