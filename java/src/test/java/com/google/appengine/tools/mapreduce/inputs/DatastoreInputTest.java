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
import com.google.appengine.api.datastore.Query;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.InputReader;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 */
public class DatastoreInputTest extends TestCase {

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

  private List<Key> populateData(int size, String namespace) {
    return populateData(size, namespace, null);
  }

  private List<Key> populateData(int size, String namespace, Key ancestor) {
    Collection<Entity> entities = new ArrayList<>(size);
    String ns = NamespaceManager.get();
    try {
      if (namespace != null) {
        NamespaceManager.set(namespace);
      }
      for (int i = 0; i < size; i++) {
        entities.add(new Entity(ENTITY_KIND_NAME, ancestor));
      }
    } finally {
      NamespaceManager.set(ns);
    }
    List<Key> keys = ds.put(entities);
    Collections.sort(keys);
    return keys;
  }

  public void testCreateQuery() {
    checkQuery("namespace", "kind");
    checkQuery(null, "kind");
  }

  private void checkQuery(String namespace, String kind) {
    Query query = BaseDatastoreInput.createQuery(namespace, kind);
    assertEquals(query.getKind(), kind);
    assertEquals(namespace == null ? "" : namespace, query.getNamespace());
  }

  public void testCreateReadersWithAncestor() throws IOException {
    List<Key> data = populateData(2, null);
    Key parrent = data.get(0);
    Key notInResult = data.get(1);
    List<Key> keys = populateData(100, null, parrent);
    DatastoreInput input = new DatastoreInput(new Query(ENTITY_KIND_NAME, parrent), 5);
    ArrayList<Key> read = new ArrayList<>();
    for (InputReader<Entity> reader : input.createReaders()) {
      read.addAll(getEntities(reader));
    }
    Collections.sort(read);
    assertFalse(read.contains(notInResult));
    assertEquals(keys.size() + 1, read.size());
    assertEquals(parrent, read.get(0));
    for (int i = 0; i < keys.size(); i++) {
      assertEquals(keys.get(i), read.get(i + 1));
    }
  }

  public void testCreateReadersWithNamespace() throws IOException {
    List<Key> keys = populateData(100, "namespace1");
    DatastoreInput input = new DatastoreInput(ENTITY_KIND_NAME, 2, "namespace1");
    InputReader<Entity> reader = input.createReaders().get(0);
    ArrayList<Key> read = new ArrayList<>();
    read.addAll(getEntities(reader));
    reader = input.createReaders().get(1);
    read.addAll(getEntities(reader));
    Collections.sort(read);
    assertEquals(keys.size(), read.size());
    for (int i = 0; i < keys.size(); i++) {
      assertEquals(keys.get(i), read.get(i));
    }
  }

  public void testCreateReadersMediumData() throws Exception {
    assertSplitting(5, (BaseDatastoreInput.RANGE_SEGMENTS_PER_SHARD / 2) * 5);
  }

  public void testCreateReadersLotsOfData() throws Exception {
    assertSplitting(5, BaseDatastoreInput.RANGE_SEGMENTS_PER_SHARD * 10 * 5);
  }

  private void assertSplitting(int numReaders, int numEntities) throws IOException {
    populateData(numEntities, null);
    List<InputReader<Entity>> splits = createReaders(numReaders);
    assertEquals(Math.max(1, Math.min(numReaders, numEntities)), splits.size());
    // Sizes are not expected to be equal due to scatter sampling
    int average = Math.round(numEntities / (float) numReaders);
    int min = (int) (average * 0.80) - 1;
    int max = (int) (average * 1.20) + 1;
    int[] counts = countEntities(splits);
    assertEquals(numEntities, total(counts));
    for (int i = 0; i < splits.size(); i++) {
      float ratio = (counts[i] / (float) average);
      assertTrue("Ratio was: " + ratio , counts[i] >= min);
      assertTrue("Ratio was: " + ratio , counts[i] <= max);
    }
  }

  private static int total(int[] ints) {
    int result = 0;
    for (int i : ints) {
      result += i;
    }
    return result;
  }

  private int[] countEntities(List<InputReader<Entity>> splits) throws IOException {
    int[] result = new int[splits.size()];
    int i = 0;
    for (InputReader<Entity> reader : splits) {
      result[i] = getEntities(reader).size();
      i++;
    }
    return result;
  }

  private List<Key> getEntities(InputReader<Entity> reader) throws IOException {
    List<Key> result = new ArrayList<>();
    reader.beginShard();
    reader.beginSlice();
    try {
      while (true) {
        result.add(reader.next().getKey());
      }
    } catch (NoSuchElementException e) {
      // Ignore
    }
    reader.endSlice();
    reader.endShard();
    return result;
  }

  public void testCreateReadersNotEnoughData() throws Exception {
    populateData(9, null);
    List<InputReader<Entity>> splits = createReaders(5);
    int[] counts = countEntities(splits);
    assertEquals(9, total(counts));
    for (int i = 0; i < counts.length; i++) {
      assertTrue(counts[i] >= 1 && counts[i] <= 6);
    }
  }

  public void testCreateReadersWithNoData() throws Exception {
    List<InputReader<Entity>> splits = createReaders(10);
    assertEquals(1, splits.size());
    InputReader<Entity> reader = splits.get(0);
    reader.beginShard();
    reader.beginSlice();
    try {
      reader.next();
      fail();
    } catch (NoSuchElementException e) {
      // Expected
    }
    reader.endSlice();
    reader.endShard();
  }

  public void testCreateReadersWithSingleKey() throws Exception {
    ds.put(new Entity(ENTITY_KIND_NAME));
    List<InputReader<Entity>> readers = createReaders(10);

    assertEquals(1, readers.size());
    List<Key> entities = getEntities(readers.get(0));
    assertEquals(1, entities.size());
  }

  public void test500() throws IOException {
    assertSplitting(2, 1000);
  }

  public void test100() throws IOException {
    assertSplitting(10, 1000);
  }

  private static List<InputReader<Entity>> createReaders(int shardCount) {
    DatastoreInput input = new DatastoreInput(ENTITY_KIND_NAME, shardCount);
    return input.createReaders();
  }
}
