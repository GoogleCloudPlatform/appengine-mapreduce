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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Tests the {@link DatastoreRecordReader} class.
 *
 * @author frew@google.com(Fred Wulff)
 */
public class DatastoreRecordReaderTest extends TestCase {
  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());
  
  private DatastoreService ds;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    ds = DatastoreServiceFactory.getDatastoreService();
  }
  
  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }
  
  /**
   * Tests that we can generate and read a simple {@link DatastoreRecordReader}.
   */
  public void testSimpleRead() throws Exception {
    Entity testEntity = new Entity("Foo");
    ds.put(testEntity);
    
    DatastoreInputSplit split = new DatastoreInputSplit(testEntity.getKey(), null);
    DatastoreRecordReader reader = new DatastoreRecordReader();
    
    reader.initialize(split, null);
    
    reader.nextKeyValue();
    assertEquals(testEntity.getKey(), reader.getCurrentKey());
    assertEquals(testEntity, reader.getCurrentValue());
  }

  /**
   * Serializes the {@link DatastoreRecordReader} and then deserializes and 
   * returns it.
   * 
   * @param split the split to use in deserialization
   * @param reader the reader to be serialized
   * @return the reconstituted reader
   * @throws IOException if the universe explodes
   */
  private DatastoreRecordReader serializeDeserialize(DatastoreInputSplit split,
      DatastoreRecordReader reader) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    reader.write(dos);
    
    reader = new DatastoreRecordReader();
    reader.initialize(split, null);
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    reader.readFields(dis);
    return reader;
  }
  
  /**
   * Test that we can serialize a {@code DatastoreRecordReader} with a split 
   * with a {@code null} end key.
   */
  public void testSerializationWithNullEndSplit() throws Exception { 
    Entity testEntity = new Entity("Foo");
    ds.put(testEntity);
    
    Entity testEntity2 = new Entity("Foo");
    ds.put(testEntity2);
    
    Entity testEntity3 = new Entity("Foo");
    ds.put(testEntity3);
    
    DatastoreInputSplit split = new DatastoreInputSplit(testEntity.getKey(), null);
    DatastoreRecordReader reader = new DatastoreRecordReader();
    
    reader.initialize(split, null);
    
    assertTrue(reader.nextKeyValue());
    assertEquals(testEntity.getKey(), reader.getCurrentKey());
    assertEquals(testEntity, reader.getCurrentValue());
    
    reader = serializeDeserialize(split, reader);
    
    assertTrue(reader.nextKeyValue());
    assertEquals(testEntity2.getKey(), reader.getCurrentKey());
    assertTrue(reader.nextKeyValue());
    assertEquals(testEntity3.getKey(), reader.getCurrentKey());
    assertFalse(reader.nextKeyValue());
  }
  
  /**
   * Test that we can serialize a {@link DatastoreRecordReader} with a split 
   * with a non-{@code null} end key.
   * @throws Exception
   */
  public void testSerializationWithEndSplit() throws Exception { 
    Entity testEntity = new Entity("Foo");
    ds.put(testEntity);
    
    Entity testEntity2 = new Entity("Foo");
    ds.put(testEntity2);
    
    Entity testEntity3 = new Entity("Foo");
    ds.put(testEntity3);
    
    DatastoreInputSplit split = new DatastoreInputSplit(testEntity.getKey(), testEntity3.getKey());
    DatastoreRecordReader reader = new DatastoreRecordReader();
    
    reader.initialize(split, null);
    
    assertTrue(reader.nextKeyValue());
    assertEquals(testEntity.getKey(), reader.getCurrentKey());
    assertEquals(testEntity, reader.getCurrentValue());
    reader = serializeDeserialize(split, reader);
    assertTrue(reader.nextKeyValue());
    assertEquals(testEntity2.getKey(), reader.getCurrentKey());
    // Split end should be exclusive
    assertFalse(reader.nextKeyValue());
  }
}
