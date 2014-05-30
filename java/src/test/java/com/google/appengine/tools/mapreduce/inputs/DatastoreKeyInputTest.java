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

import junit.framework.TestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 */
public class DatastoreKeyInputTest extends TestCase {

  static final String ENTITY_KIND_NAME = "Bob";

  private final LocalServiceTestHelper helper
      = new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());
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

  private void populateData(int size, String namespace) {
    Collection<Entity> entities = new ArrayList<>(size);
    String ns = NamespaceManager.get();
    try {
      if (namespace != null) {
        NamespaceManager.set(namespace);
      }
      for (int i = 0; i < size; i++) {
        entities.add(new Entity(ENTITY_KIND_NAME, "key_" + i));
      }
    } finally {
      NamespaceManager.set(ns);
    }
    List<Key> keys = ds.put(entities);
    Collections.sort(keys);
  }

  public void testCreateReadersWithNamespace() throws IOException {
    populateData(10, "namespace1");
    DatastoreKeyInput input = new DatastoreKeyInput(ENTITY_KIND_NAME, 1, "namespace1");
    DatastoreKeyInputReader reader = input.createReaders().get(0);
    verifyReader(reader, 10, "namespace1");
  }

  private void verifyReader(DatastoreKeyInputReader reader, int size, String namespace)
      throws IOException {
    reader.beginShard();
    reader.beginSlice();
    for (int i = 0; i < size; i++) {
      Key key = reader.next();
      assertEquals("key_" + i, key.getName());
      assertEquals(namespace, key.getNamespace());
    }
    try {
      reader.next();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException expected) {
      //
    }
    reader.endSlice();
    reader.endShard();
  }

  public void testCreateReaders() throws Exception {
    populateData(10, null);
    DatastoreKeyInput input = new DatastoreKeyInput(ENTITY_KIND_NAME, 1);
    DatastoreKeyInputReader reader = input.createReaders().get(0);
    verifyReader(reader, 10, "");
  }
}
