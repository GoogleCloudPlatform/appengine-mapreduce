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
class LineInputReader extends StreamInputReader<byte[]> {

  private static final long serialVersionUID = -1047037533107945060L;
  private static final Logger log = Logger.getLogger(LineInputReader.class.getName());
  private static final int INITIAL_BUFFER_SIZE = 10000;

  private final boolean skipFirstTerminator;
  private final byte separator;

  // Note: length may be a negative value when we are reading beyond the split boundary.
  LineInputReader(
      InputStream input, long length, boolean skipFirstTerminator, byte separator) {
    super(input, length);
    this.skipFirstTerminator = skipFirstTerminator;
    this.separator = separator;
  }

  @Override
  public byte[] next() {
    try {
      if (getBytesRead() == 0 && skipFirstTerminator) {
        // skip the first record
        copyUntilNextRecord(null);
      }
      // we are reading one record after split-end
      // and are skipping first record for all splits except for the leading one.
      // check if we read one byte ahead of the split.
      if (getBytesRead() - 1 >= length) {
        throw new NoSuchElementException();
      }
      long recordStart = getBytesRead();
      ByteArrayOutputStream tempBuffer = new ByteArrayOutputStream(INITIAL_BUFFER_SIZE);
      copyUntilNextRecord(tempBuffer);
      return  tempBuffer.toByteArray();
    } catch (IOException e) {
      log.log(Level.WARNING, "Failed to read next record", e);
      throw new RuntimeException("Failed to read next record", e);
    }
  }

  private void copyUntilNextRecord(OutputStream out) throws IOException {
    int value = read();
    if (value == -1) {
      throw new NoSuchElementException();
    }
    while ((value != (separator & 0xff)) && value != -1) {
      if (out != null) {
        out.write(value);
      }
      value = read();
    }
  }

}
