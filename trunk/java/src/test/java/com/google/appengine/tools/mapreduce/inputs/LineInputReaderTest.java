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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.CountingInputStream;
import com.google.common.primitives.Bytes;

import junit.framework.TestCase;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Unit test for {@code InputStreamIterator}.
 *
 */
public class LineInputReaderTest extends TestCase {

  private static class TestLineInputReader extends LineInputStream {

    private boolean skipFirstTerminator;

    TestLineInputReader(InputStream input, long length, boolean skipFirstTerminator,
        byte separator) {
      super(new CountingInputStream(input), length, separator);
      this.skipFirstTerminator = skipFirstTerminator;
    }

    @Override
    public byte[] next() {
      if (getBytesCount() == 0 && skipFirstTerminator) {
        super.next();
      }
      return super.next();
    }
  }

// ------------------------------ FIELDS ------------------------------

  InputStream input;
  private final List<String> content =
      ImmutableList.of("I", "am", "RecordReader", "Hello", "", "world", "!");
  private final List<Long> byteContentOffsets = Lists.newArrayListWithCapacity(content.size());

// ------------------------ OVERRIDING METHODS ------------------------

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    byte[] terminator = new byte[] { -1 };
    byte[] byteContent = new byte[0];
    for (String str : content) {
      byteContentOffsets.add((long) byteContent.length);
      byteContent = Bytes.concat(byteContent, str.getBytes(), terminator);
    }
    input = new BufferedInputStream(new NonResetableByteArrayInputStream(byteContent));
  }

// -------------------------- TEST METHODS --------------------------

  /** Tests leading split the size of the record. Should return this and the next record. */
  public void testLeadingGreaterThanRecord() throws Exception {
    int startIndex = 3;
    long start = byteContentOffsets.get(startIndex);
    test(start, byteContentOffsets.get(startIndex + 1),
        false, startIndex, startIndex + 2);
  }

  /** Tests leading split smaller than a record. Should return one record. */
  public void testLeadingSmallerThanRecord() throws Exception {
    int startIndex = 3;
    long start = byteContentOffsets.get(startIndex);
    test(start, start + 2, false, startIndex, startIndex + 1);
  }

  /** Tests iterating over all items. */
  public void testAllItems() throws Exception {
    test(0, Long.MAX_VALUE, false, 0, content.size());
  }

  /** Tests leading split of length 0 with an empty record. Should return this record. */
  public void testLeadingWithEmptyRecord() throws Exception {
    int startIndex = 4;
    long start = byteContentOffsets.get(startIndex);
    test(start, start, false, startIndex, startIndex + 1);
  }

  /** Tests non-leading split the size of the record. Should return next record. */
  public void testNonLeadingGreaterThanRecord() throws Exception {
    int startIndex = 3;
    long start = byteContentOffsets.get(startIndex);
    test(start, byteContentOffsets.get(startIndex + 1),
        true, startIndex + 1, startIndex + 2);
  }

  /** Tests non-leading split smaller than a record. Should return no records. */
  public void testNonLeadingSmallerThanRecord() throws Exception {
    int startIndex = 3;
    long start = byteContentOffsets.get(startIndex);
    test(start, start + 2, true, startIndex, startIndex);
  }

  /**
   * Tests non-leading split starting at the previous record. Should return this and the next
   * records.
   */
  public void testNonLeadingStartingAtRecord() throws Exception {
    int startIndex = 3;
    long start = byteContentOffsets.get(startIndex);
    test(start - 2, byteContentOffsets.get(startIndex + 1),
        true, startIndex, startIndex + 2);
  }

  /**
   * Tests non-leading split starting at the terminator of the previous record. Should return
   * this and the next records.
   */
  public void testNonLeadingStartingAtRecordTerminator() throws Exception {
    int startIndex = 3;
    long start = byteContentOffsets.get(startIndex);
    test(start - 1, byteContentOffsets.get(startIndex + 1),
        true, startIndex, startIndex + 2);
  }

  public void testExceptionHandling() throws Exception {
    // Create an input stream than has 2 records in the first 9 bytes and exception while the 3rd
    // record is read
    byte[] content = new byte[] {1, 2, 3, 4, 0, 6, 7, 8, 0, 10, 11, 12};
    CountingInputStream countingInputStream =
        new CountingInputStream(new ExceptionThrowingInputStream(
            new BufferedInputStream(new NonResetableByteArrayInputStream(content)), 11));
    LineInputStream iterator =
        new TestLineInputReader(countingInputStream, content.length, false, (byte) 0);

    byte[] next = iterator.next();
    assertNotNull(next);
    next = iterator.next();
    assertNotNull(next);
    try {
      iterator.next();
      fail("Exception was not passed through");
    } catch (RuntimeException expected) {
    }
    countingInputStream.close();
  }

// -------------------------- INSTANCE METHODS --------------------------

  private void test(long start, long end, boolean skipFirstTerminator, int expectedIndexStart,
      int expectedIndexEnd) throws IOException {
    input.skip(start);
    CountingInputStream countingInputStream = new CountingInputStream(input);
    LineInputStream iterator =
        new TestLineInputReader(countingInputStream, end - start, skipFirstTerminator, (byte) -1);
    int totalCount = 0;
    try {
      while (true) {
        byte[] record = iterator.next();
        assertEquals(content.get(totalCount + expectedIndexStart), new String(record));
        assertEquals(byteContentOffsets.get(totalCount + expectedIndexStart).longValue(),
            countingInputStream.getCount() - record.length - 1 + start);
        totalCount++;
      }
    } catch (NoSuchElementException e) {
      // Used as break
    }
    assertEquals("pairs between " + start + " and " + end, expectedIndexEnd - expectedIndexStart,
        totalCount);
  }

// -------------------------- INNER CLASSES --------------------------

  /**
   * Wrapper class for {@code ByteArrayInputStream} to double check that an InputStreamIterator
   * applied to a BufferedInputStream doesn't call mark() and reset() on the underlying InputStream.
   * Should be obvious, but doesn't hurt to test.
   */
  public class NonResetableByteArrayInputStream extends ByteArrayInputStream {
    public NonResetableByteArrayInputStream(byte[] array) {
      super(array);
    }

    @Override
    public void mark(int readAheadLimit) {
      fail("Tried to call mark() on the underlying InputStream");
    }

    @Override
    public synchronized void reset() {
      fail("Tried to call reset() on the underlying InputStream");
    }
  }

  /**
   * Wrapper class for InputStream that throws an IOException after the specified number of reads
   */
  private static class ExceptionThrowingInputStream extends InputStream {

    private InputStream inputStream;
    private int readsBetweenExceptions;
    private int reads;

    public ExceptionThrowingInputStream(InputStream inputStream, int readBetweenExceptions) {
      this.inputStream = inputStream;
      this.readsBetweenExceptions = readBetweenExceptions;
      this.reads = 0;
    }

    @Override
    public int read() throws IOException {
      reads++;
      if (reads % readsBetweenExceptions == 0) {
        throw new IOException();
      } else {
        return inputStream.read();
      }
    }
  }
}
