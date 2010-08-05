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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

/**
 * Tests both RangeRecordReader and RangeInputSplit (since the latter is light 
 * on logic).
 * 
 * @author frew@google.com (Fred Wulff)
 * 
 */
public class RangeRecordReaderTest extends TestCase {
  protected void setUp() throws Exception {
    super.setUp();
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }
  
  /**
   * Serializes the {@link RangeRecordReader} and then deserializes and 
   * returns it.
   * 
   * @param split the split to use in deserialization
   * @param reader the reader to be serialized
   * @return the reconstituted reader
   * @throws IOException if the universe explodes
   */
  private RangeRecordReader serializeDeserializeReader(RangeInputSplit split,
      RangeRecordReader reader) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    reader.write(dos);
    
    reader = new RangeRecordReader();
    reader.initialize(split, null);
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    reader.readFields(dis);
    return reader;
  }
  
  public void testSerializeSplit() throws Exception {
    RangeInputSplit oldSplit = new RangeInputSplit(1, 5);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    oldSplit.write(dos);
    
    RangeInputSplit newSplit = new RangeInputSplit();
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    newSplit.readFields(dis);
    assertEquals(1, newSplit.getSplitStart());
    assertEquals(5, newSplit.getSplitEnd());
  }
  
  public void testSerializeReader() throws Exception {
    RangeInputSplit split = new RangeInputSplit(1, 3);
    RangeRecordReader reader = new RangeRecordReader();
    reader.initialize(split, null);
    assertNull(reader.getCurrentKey());
    reader = serializeDeserializeReader(split, reader);
    assertNull(reader.getCurrentKey());
    assertTrue(reader.nextKeyValue());
    assertEquals(new Long(1), reader.getCurrentKey());
    reader = serializeDeserializeReader(split, reader);
    assertEquals(new Long(1), reader.getCurrentKey());
    assertTrue(reader.nextKeyValue());
    assertEquals(new Long(2), reader.getCurrentKey());
    reader = serializeDeserializeReader(split, reader);
    assertEquals(new Long(2), reader.getCurrentKey());
    assertFalse(reader.nextKeyValue());
    assertNull(reader.getCurrentKey());
    reader = serializeDeserializeReader(split, reader);
    assertNull(reader.getCurrentKey()); 
  }
  
  public void testEmptyReader() throws Exception {
    RangeInputSplit split = new RangeInputSplit(1, 1);
    RangeRecordReader reader = new RangeRecordReader();
    reader.initialize(split, null);
    assertNull(reader.getCurrentKey());
    assertFalse(reader.nextKeyValue());
    assertNull(reader.getCurrentKey());
  }
}
