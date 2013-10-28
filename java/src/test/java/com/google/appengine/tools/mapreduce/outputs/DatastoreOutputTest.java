/*
 * Copyright 2013 Google Inc.
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
package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.OutputWriter;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

/**
 * Tests for {@link DatastoreOutput}.
 */
public class DatastoreOutputTest extends TestCase {

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());

  private Entity entity1;
  private Entity entity2;
  private Entity entity3;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    entity1 = new Entity("kind1", "v1");
    entity2 = new Entity("kind1", "v2");
    entity3 = new Entity("kind1", "v3");
  }

  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

  public void testCreateWriters() {
    DatastoreOutput output = new DatastoreOutput(3);
    List<? extends OutputWriter<Entity>> writers = output.createWriters();
    assertEquals(3, writers.size());
  }

  public void testFinish() {
    DatastoreOutput output = new DatastoreOutput(1);
    List<? extends OutputWriter<Entity>> writers = output.createWriters();
    assertEquals(1, writers.size());
    assertNull(output.finish(writers));
  }

  public void testDatastoreOutputWriter()
      throws IOException, ClassNotFoundException, EntityNotFoundException {
    DatastoreOutput output = new DatastoreOutput(1);
    OutputWriter<Entity> writer = output.createWriters().get(0);
    writer.open();
    writer.beginSlice();
    writer.write(entity1);
    writer = reconstruct(writer);
    writer.beginSlice();
    writer.write(entity2);
    writer.write(entity3);
    writer.endSlice();
    writer.close();

    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    try {
      ds.get(entity1.getKey());
      fail("Should ot be able to find entity1");
    } catch (EntityNotFoundException ignore) {
      // expected
    }
    assertEquals(entity2, ds.get(entity2.getKey()));
    assertEquals(entity3, ds.get(entity3.getKey()));
  }

  @SuppressWarnings("unchecked")
  private OutputWriter<Entity> reconstruct(OutputWriter<Entity> writer) throws IOException,
      ClassNotFoundException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ObjectOutputStream oout = new ObjectOutputStream(bout);
    oout.writeObject(writer);
    oout.close();
    assertTrue(bout.size() < 1000 * 1000); // Should fit in datastore.
    ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
    ObjectInputStream oin = new ObjectInputStream(bin);
    return (OutputWriter<Entity>) oin.readObject();
  }
}
