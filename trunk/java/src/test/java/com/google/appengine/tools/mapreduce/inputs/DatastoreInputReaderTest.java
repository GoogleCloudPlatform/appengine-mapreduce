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

import com.google.appengine.api.NamespaceManager;
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
import java.util.List;
import java.util.NoSuchElementException;

/**
 */
public class DatastoreInputReaderTest extends TestCase {

  static final String ENTITY_KIND_NAME = "Bob";

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

  public void testOneEntityOnly() throws Exception {
    Key key = populateData(1, null).get(0);
    DatastoreInputReader reader = new DatastoreInputReader(ENTITY_KIND_NAME, key, null, null);
    reader.beginSlice();
    assertEquals(key, reader.next().getKey());
    try {
      reader.next();
      fail();
    } catch (NoSuchElementException e) {
      // ok
    }
  }

  private List<Key> populateData(int size, String namespace) {
    Collection<Entity> entities = new ArrayList<>(size);
    String ns = NamespaceManager.get();
    try {
      if (namespace != null) {
        NamespaceManager.set(namespace);
      }
      for (int i = 0; i < size; i++) {
        entities.add(new Entity(ENTITY_KIND_NAME));
      }
    } finally {
      NamespaceManager.set(ns);
    }
    List<Key> keys = ds.put(entities);
    Collections.sort(keys);
    return keys;
  }

  public void testReadAllData() throws Exception {
    List<Key> keys = populateData(300, null);
    validateReadAllData(keys);
  }

  public void testReadAllDataWithNamespace() throws Exception {
    List<Key> keys = populateData(300, "namespace1");
    validateReadAllData(keys);
  }

  private void validateReadAllData(List<Key> keys) {
    Key start = keys.get(0);
    DatastoreInputReader reader =
        new DatastoreInputReader(ENTITY_KIND_NAME, start, null, start.getNamespace());
    List<Key> readKeys = readAllFromReader(reader);
    assertEquals(keys.size(), readKeys.size());
    assertEquals(keys, readKeys);
  }

  private List<Key> readAllFromReader(DatastoreInputReader reader) {
    List<Key> readKeys = new ArrayList<>();
    reader.beginSlice();
    while (true) {
      try {
        Entity entity = reader.next();
        readKeys.add(entity.getKey());
      } catch (NoSuchElementException e) {
        return readKeys;
      }
    }
  }

  public void testSerialization() throws Exception {
    List<Key> keys = populateData(300, null);
    List<Key> readKeys = new ArrayList<>();
    DatastoreInputReader reader =
        new DatastoreInputReader(ENTITY_KIND_NAME, keys.get(100), keys.get(200), null);
    reader.beginSlice();
    while (true) {
      try {
        Entity entity = reader.next();
        readKeys.add(entity.getKey());
      } catch (NoSuchElementException e) {
        break;
      }

      reader.endSlice();
      byte[] bytes = SerializationUtil.serializeToByteArray(reader);
      reader = (DatastoreInputReader) SerializationUtil.deserializeFromByteArray(bytes);
      reader.beginSlice();
    }

    assertEquals(100, readKeys.size());
    assertEquals(keys.subList(100, 200), readKeys);
  }

  public void testStartEndKey() throws Exception {
    List<Key> keys = populateData(300, null);
    DatastoreInputReader reader =
        new DatastoreInputReader(ENTITY_KIND_NAME, keys.get(100), keys.get(200), null);
    List<Key> readKeys = readAllFromReader(reader);
    assertEquals(100, readKeys.size());
    assertEquals(keys.subList(100, 200), readKeys);
  }
}
