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
import java.io.IOException;

/**
 */
public class DatastoreSerializationUtilTest extends TestCase {
  /**
   * Test that we can serialize and reconstitute {@code null} as a
   * datastore key.
   */
  public void testSerializeDeserializeNull() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    DatastoreSerializationUtil.writeKeyOrNull(dos, null);
    dos.close();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    assertNull(DatastoreSerializationUtil.readKeyOrNull(dis));
  }
}
