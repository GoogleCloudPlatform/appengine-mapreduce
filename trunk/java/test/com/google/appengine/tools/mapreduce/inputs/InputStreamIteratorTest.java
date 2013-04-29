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

/**
 * Unit test for {@code InputStreamIterator}.
 *
 */
public class InputStreamIteratorTest extends TestCase {
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
  public void test_LeadingGreaterThanRecord() throws Exception {
    int startIndex = 3;
    long start = byteContentOffsets.get(startIndex);
    test(start, byteContentOffsets.get(startIndex + 1),
        false, startIndex, startIndex + 2);
  }

  /** Tests leading split smaller than a record. Should return one record. */
  public void test_LeadingSmallerThanRecord() throws Exception {
    int startIndex = 3;
    long start = byteContentOffsets.get(startIndex);
    test(start, start + 2, false, startIndex, startIndex + 1);
  }

  /** Tests iterating over all items. */
  public void test_allItems() throws Exception {
    test(0, Long.MAX_VALUE, false, 0, content.size());
  }

  /** Tests leading split of length 0 with an empty record. Should return this record. */
  public void test_leadingWithEmptyRecord() throws Exception {
    int startIndex = 4;
    long start = byteContentOffsets.get(startIndex);
    test(start, start, false, startIndex, startIndex + 1);
  }

  /** Tests non-leading split the size of the record. Should return next record. */
  public void test_nonLeadingGreaterThanRecord() throws Exception {
    int startIndex = 3;
    long start = byteContentOffsets.get(startIndex);
    test(start, byteContentOffsets.get(startIndex + 1),
        true, startIndex + 1, startIndex + 2);
  }

  /** Tests non-leading split smaller than a record. Should return no records. */
  public void test_nonLeadingSmallerThanRecord() throws Exception {
    int startIndex = 3;
    long start = byteContentOffsets.get(startIndex);
    test(start, start + 2, true, startIndex, startIndex);
  }

  /**
   * Tests non-leading split starting at the previous record. Should return this and the next
   * records.
   */
  public void test_nonLeadingStartingAtRecord() throws Exception {
    int startIndex = 3;
    long start = byteContentOffsets.get(startIndex);
    test(start - 2, byteContentOffsets.get(startIndex + 1),
        true, startIndex, startIndex + 2);
  }

  /**
   * Tests non-leading split starting at the terminator of the previous record. Should return
   * this and the next records.
   */
  public void test_nonLeadingStartingAtRecordTerminator() throws Exception {
    int startIndex = 3;
    long start = byteContentOffsets.get(startIndex);
    test(start - 1, byteContentOffsets.get(startIndex + 1),
        true, startIndex, startIndex + 2);
  }

  /**
   * Makes sure that the basic method of offset record pair behave correctly,
   */
  public void test_offsetRecordPair() throws Exception {
    byte[] testArray1 = new byte[]{0x20, 0x40};
    // Logically equal to array 1, but not referentially equal
    byte[] testArray1copy = new byte[]{0x20, 0x40};
    byte[] testArray2 = new byte[]{0x30};
    byte[] testArray3 = new byte[0];
    long offset1 = 10;
    long offset2 = 4;
    InputStreamIterator.OffsetRecordPair pair11 = new InputStreamIterator.OffsetRecordPair(
        offset1, testArray1);
    InputStreamIterator.OffsetRecordPair pair21 = new InputStreamIterator.OffsetRecordPair(
        offset2, testArray1);
    InputStreamIterator.OffsetRecordPair pair11copy = new InputStreamIterator.OffsetRecordPair(
        offset1, testArray1copy);
    InputStreamIterator.OffsetRecordPair pair12 = new InputStreamIterator.OffsetRecordPair(
        offset1, testArray2);
    InputStreamIterator.OffsetRecordPair pair13 = new InputStreamIterator.OffsetRecordPair(
        offset1, testArray3);
    assertEquals(pair11, pair11copy);
    assertFalse(pair11.equals(pair21));
    assertFalse(pair11.equals(pair12));
    assertFalse(pair11.equals(pair13));
    assertFalse(pair11.equals("foo"));
    assertFalse(pair11.equals(null));
    assertEquals(pair11.hashCode(), pair11copy.hashCode());
  }

// -------------------------- INSTANCE METHODS --------------------------

  private List<InputStreamIterator.OffsetRecordPair> readPairs(
      long start, long end, boolean skipFirstTerminator)
      throws IOException {
    input.skip(start);
    InputStreamIterator iterator = new InputStreamIterator(new CountingInputStream(input),
        end - start, skipFirstTerminator, (byte) -1);
    return ImmutableList.copyOf(iterator);
  }

  private void test(long start, long end, boolean skipFirstTerminator, int expectedIndexStart,
      int expectedIndexEnd) throws IOException {
    List<InputStreamIterator.OffsetRecordPair> pairs = readPairs(start, end, skipFirstTerminator);
    assertEquals("pairs between " + start + " and " + end, expectedIndexEnd - expectedIndexStart,
        pairs.size());
    for (int i = 0; i < pairs.size(); i++) {
      assertEquals(content.get(i + expectedIndexStart), new String(pairs.get(i).getRecord()));
      assertEquals(byteContentOffsets.get(i + expectedIndexStart).longValue(),
          pairs.get(i).getOffset() + start);
    }
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
    public void reset() {
      fail("Tried to call reset() on the underlying InputStream");
    }
  }
}
