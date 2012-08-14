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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 */
public class DatastoreInputReaderTest extends TestCase {
// --------------------------- STATIC FIELDS ---------------------------

  static final String ENTITY_KIND_NAME = "Bob";

// ------------------------------ FIELDS ------------------------------

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());

  private DatastoreService ds;

// ------------------------ OVERRIDING METHODS ------------------------

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

// -------------------------- TEST METHODS --------------------------

  public void testNoData() throws Exception {
    DatastoreInputReader reader = new DatastoreInputReader(ENTITY_KIND_NAME, null, null);
    reader.beginSlice();
    try {
      reader.next();
      fail();
    } catch (NoSuchElementException e) {
      // ok
    }
  }

  public void testReadAllData() throws Exception {
    Collection<Entity> entities = new ArrayList<Entity>();
    for (int i = 0; i < 300; i++) {
      entities.add(new Entity(ENTITY_KIND_NAME));
    }

    Set<Key> keys = new HashSet<Key>(ds.put(entities));
    Set<Key> readKeys = new HashSet<Key>();
    DatastoreInputReader reader = new DatastoreInputReader(ENTITY_KIND_NAME, null, null);
    reader.beginSlice();
    while (true) {
      Entity entity;
      try {
        entity = reader.next();
      } catch (NoSuchElementException e) {
        break;
      }
      readKeys.add(entity.getKey());
    }

    assertEquals(keys.size(), readKeys.size());
    assertEquals(keys, readKeys);
  }

  public void testSerialization() throws Exception {
    Collection<Entity> entities = new ArrayList<Entity>();
    for (int i = 0; i < 300; i++) {
      entities.add(new Entity(ENTITY_KIND_NAME));
    }

    List<Key> keys = ds.put(entities);
    Collections.sort(keys);

    List<Key> readKeys = new ArrayList<Key>();

    DatastoreInputReader reader = new DatastoreInputReader(ENTITY_KIND_NAME,
        keys.get(100), keys.get(200));
    reader.beginSlice();
    while (true) {
      Entity entity;
      try {
        entity = reader.next();
      } catch (NoSuchElementException e) {
        break;
      }
      readKeys.add(entity.getKey());

      reader.endSlice();
      byte[] bytes = SerializationUtil.serializeToByteArray(reader);
      reader = (DatastoreInputReader) SerializationUtil.deserializeFromByteArray(bytes);
      reader.beginSlice();
    }

    assertEquals(100, readKeys.size());
    assertEquals(keys.subList(100, 200), readKeys);
  }

  public void testStartEndKey() throws Exception {
    Collection<Entity> entities = new ArrayList<Entity>();
    for (int i = 0; i < 300; i++) {
      entities.add(new Entity(ENTITY_KIND_NAME));
    }

    List<Key> keys = ds.put(entities);
    Collections.sort(keys);

    List<Key> readKeys = new ArrayList<Key>();

    DatastoreInputReader reader = new DatastoreInputReader(ENTITY_KIND_NAME,
        keys.get(100), keys.get(200));
    reader.beginSlice();
    while (true) {
      Entity entity;
      try {
        entity = reader.next();
      } catch (NoSuchElementException e) {
        break;
      }
      readKeys.add(entity.getKey());
    }

    assertEquals(100, readKeys.size());
    assertEquals(keys.subList(100, 200), readKeys);
  }
}
