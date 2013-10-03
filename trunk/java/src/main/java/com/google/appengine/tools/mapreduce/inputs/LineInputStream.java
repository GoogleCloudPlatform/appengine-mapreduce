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

import com.google.apphosting.api.AppEngineInternal;
import com.google.common.io.CountingInputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A reader to return one line at a time from an underlying input stream.
 *
 */
@AppEngineInternal
class LineInputStream {

  private static final Logger log = Logger.getLogger(LineInputStream.class.getName());
  private static final int INITIAL_BUFFER_SIZE = 10000;

  private final int separator;
  private final CountingInputStream in;
  private final long lengthToRead;

  LineInputStream(InputStream in, long lengthToRead, byte separator) {
    this.in = new CountingInputStream(in);
    this.lengthToRead = lengthToRead;
    this.separator = (separator & 0xff);
  }

  public byte[] next() {
    try {
      // we are reading one record after lengthToRead because the splits are not assumed to be
      // aligned to separators in the file.
      long recordStart = in.getCount();
      if (recordStart - 1 >= lengthToRead) {
        throw new NoSuchElementException();
      }
      ByteArrayOutputStream tempBuffer = new ByteArrayOutputStream(INITIAL_BUFFER_SIZE);
      copyUntilNextRecord(tempBuffer);
      return tempBuffer.toByteArray();
    } catch (IOException e) {
      log.log(Level.WARNING, "Failed to read next record", e);
      throw new RuntimeException("Failed to read next record", e);
    }
  }

  public long getBytesCount() {
    return in.getCount();
  }

  private void copyUntilNextRecord(OutputStream out) throws IOException {
    int value = in.read();
    if (value == -1) {
      throw new NoSuchElementException();
    }
    while (value != separator && value != -1) {
      out.write(value);
      value = in.read();
    }
  }

  public void close() throws IOException {
    in.close();
  }

}
