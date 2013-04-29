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
import com.google.common.collect.AbstractIterator;
import com.google.common.io.CountingInputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An iterator iterating over records in an input stream.
 *
 */
class InputStreamIterator extends AbstractIterator<InputStreamIterator.OffsetRecordPair> {

  private static final Logger log = Logger.getLogger(InputStreamIterator.class.getName());
  private static final int INITIAL_BUFFER_SIZE = 10000;

  private final CountingInputStream input;
  private final long length;
  private final boolean skipFirstTerminator;
  private final byte separator;

  // Note: length may be a negative value when we are reading beyond the split boundary.
  InputStreamIterator(
      CountingInputStream input, long length, boolean skipFirstTerminator, byte separator) {
    this.input = checkNotNull(input, "Null input");
    this.length = length;
    this.skipFirstTerminator = skipFirstTerminator;
    this.separator = separator;
  }

  @Override
  public OffsetRecordPair computeNext() {
    try {
      if (input.getCount() == 0 && skipFirstTerminator) {
        // skip the first record
        copyUntilNextRecord(input, null);
      }
      // we are reading one record after split-end
      // and are skipping first record for all splits except for the leading one.
      // check if we read one byte ahead of the split.
      if (input.getCount() - 1 >= length) {
        return endOfData();
      }
      long recordStart = input.getCount();
      ByteArrayOutputStream tempBuffer = new ByteArrayOutputStream(INITIAL_BUFFER_SIZE);
      if (!copyUntilNextRecord(input, tempBuffer)) {
        return endOfData();
      }
      byte[] byteValue = tempBuffer.toByteArray();
      return new OffsetRecordPair(recordStart, byteValue);
    } catch (IOException e) {
      log.log(Level.WARNING, "Failed to read next record", e);
      return endOfData();
    }
  }

  /**
   * @return false iff was unable to read anything because the end of the InputStream was reached.
   */
  private boolean copyUntilNextRecord(InputStream in, OutputStream out) throws IOException {
    int value = in.read();
    if (value == -1) {
      return false;
    }
    while ((value != (separator & 0xff)) && value != -1) {
      if (out != null) {
        out.write(value);
      }
      value = in.read();
    }
    return true;
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
