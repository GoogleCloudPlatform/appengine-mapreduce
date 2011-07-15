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

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * Unit test for {@code BlobstoreInputSplit}.
 *
 */
public class BlobstoreInputSplitTest extends TestCase {

  /** Tests equality of BlobstoreInputSplit */
  public void test_Equals() throws Exception {
    BlobstoreInputSplit splitA = new BlobstoreInputSplit("foo", 100, 1000);
    BlobstoreInputSplit splitB = new BlobstoreInputSplit("foo", 100, 1000);
    BlobstoreInputSplit splitC = new BlobstoreInputSplit("heh", 100, 1000);

    assertEquals(splitA, splitB);
    assertFalse(splitB.equals(splitC));
  }

  /** Tests that restored split is equal to the stored one */
  public void test_Persistence() throws Exception {
    BlobstoreInputSplit splitA = new BlobstoreInputSplit("foo", 100, 1000);
    ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(byteArrayOutput);
    splitA.write(out);

    DataInputStream in = new DataInputStream(
        new ByteArrayInputStream(byteArrayOutput.toByteArray()));

    BlobstoreInputSplit splitB = new BlobstoreInputSplit();
    splitB.readFields(in);
    assertEquals(splitA, splitB);
  }
}
