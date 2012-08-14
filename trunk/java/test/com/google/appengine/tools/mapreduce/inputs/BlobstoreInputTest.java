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

import com.google.appengine.tools.mapreduce.InputReader;

import java.util.List;

/**
 * Unit test for {@code BlobstoreInput}.
 */
public class BlobstoreInputTest extends BlobstoreInputTestCase {
// -------------------------- TEST METHODS --------------------------

  public void testSplit() throws Exception {
    BlobstoreInput input = new BlobstoreInput(blobKey.getKeyString(), (byte) '\n', 4);
    List<? extends InputReader<byte[]>> readers = input.createReaders();
    assertEquals(4, readers.size());
    assertSplitRange(0, 300, readers.get(0));
    assertSplitRange(300, 600, readers.get(1));
    assertSplitRange(600, 900, readers.get(2));
    assertSplitRange(900, 1200, readers.get(3));
  }

// -------------------------- STATIC METHODS --------------------------

  private static void assertSplitRange(int start, int end, InputReader<byte[]> reader) {
    BlobstoreInputReader r = (BlobstoreInputReader) reader;
    assertEquals("Start offset mismatch", start, r.startOffset);
    assertEquals("End offset mismatch", end, r.endOffset);
  }
}
