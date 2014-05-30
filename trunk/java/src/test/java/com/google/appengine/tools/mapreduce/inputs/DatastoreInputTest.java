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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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

  public void testCreateReadersWithNamespace() {
    List<Key> keys = populateData(100, "namespace1");
    DatastoreInput input = new DatastoreInput(ENTITY_KIND_NAME, 2, "namespace1");
    DatastoreInputReader reader = input.createReaders().get(0);
    assertStartAndEndKeys(reader, keys.get(0), keys.get(43));
    reader.beginSlice();
    for (Key key : keys.subList(0, 43)) {
      assertEquals("namespace1", key.getNamespace());
      assertEquals(key, reader.next().getKey());
    }
    reader = input.createReaders().get(1);
    assertStartAndEndKeys(reader, keys.get(43), null);
    reader.beginSlice();
    for (Key key : keys.subList(43, keys.size())) {
      assertEquals("namespace1", key.getNamespace());
      assertEquals(key, reader.next().getKey());
    }
  }

  public void testCreateReadersLotsOfData() throws Exception {
    List<Key> keys = populateData(300, null);
    List<DatastoreInputReader> splits = createReaders(5);
    assertEquals(5, splits.size());
    // Sizes are not expected to be equal due to scatter sampling
    // Expected sizes for this dataset are: 62, 69, 65, 52, and 52
    assertStartAndEndKeys(splits.get(0), keys.get(0), keys.get(62));
    assertStartAndEndKeys(splits.get(1), keys.get(62), keys.get(131));
    assertStartAndEndKeys(splits.get(2), keys.get(131), keys.get(196));
    assertStartAndEndKeys(splits.get(3), keys.get(196), keys.get(248));
    assertStartAndEndKeys(splits.get(4), keys.get(248), null);
  }

  public void testCreateReadersNotEnoughData() throws Exception {
    List<Key> keys = populateData(9, null);
    List<DatastoreInputReader> splits = createReaders(5);
    assertEquals(4, splits.size());
    assertStartAndEndKeys(splits.get(0), keys.get(0), keys.get(6));
    assertStartAndEndKeys(splits.get(1), keys.get(6), keys.get(7));
    assertStartAndEndKeys(splits.get(2), keys.get(7), keys.get(8));
    assertStartAndEndKeys(splits.get(3), keys.get(8), null);
  }

  public void testCreateReadersWithNoData() throws Exception {
    List<DatastoreInputReader> splits = createReaders(10);
    assertEquals(0, splits.size());
  }

  public void testCreateReadersWithSingleKey() throws Exception {
    Key key = ds.put(new Entity(ENTITY_KIND_NAME));
    List<DatastoreInputReader> splits = createReaders(1);

    assertEquals(1, splits.size());
    assertStartAndEndKeys(splits.get(0), key, null);
  }

  public void testSplitFairness() throws Exception {
    assertRegionSizeBounds(0, 1);
    assertRegionSizeBounds(1, 2);
    assertRegionSizeBounds(2, 2);
    assertRegionSizeBounds(7, 2);
    assertRegionSizeBounds(999, 2);
    assertRegionSizeBounds(999, 10);
    assertRegionSizeBounds(999, 75);
    assertRegionSizeBounds(999, 100);
    assertRegionSizeBounds(999, 1000);
  }

  private static void assertStartAndEndKeys(DatastoreInputReader reader, Key startKey, Key endKey) {
    assertEquals("Start key doesn't match", startKey, reader.startKey);
    assertEquals("End key doesn't match", endKey, reader.endKey);
  }

  private static List<DatastoreInputReader> createReaders(int shardCount) {
    DatastoreInput input = new DatastoreInput(ENTITY_KIND_NAME, shardCount);
    return input.createReaders();
  }

  private static void assertRegionSizeBounds(int scatterKeys, int numShards) {
    // Each shard should have the same number of regions, however since we round to the nearest
    // whole number the max size may be one larger than the min if the optimal number of regions
    // is a decimal value. The number of regions is 1 larger than the number of scatter keys.
    int min = (scatterKeys + 1) / numShards;
    int max = min + ((((scatterKeys + 1) % numShards) > 0) ? 1 : 0);

    ArrayList<Entity> entities = new ArrayList<>(scatterKeys);
    for (int i = 1; i <= scatterKeys; i++) {
      entities.add(new Entity(ENTITY_KIND_NAME, i));
    }
    Iterable<Key> splitKeys = BaseDatastoreInput.chooseSplitPoints(entities, numShards);
    // We start with 0 since the first shard includes the region before the first scatter key
    long lastId = 0;
    for (Key key : splitKeys) {
      int shardSize = (int) (key.getId() - lastId);
      lastId = key.getId();
      assertTrue("Too many regions " + shardSize + ">" + max, shardSize <= max);
      assertTrue("Too few regions " + shardSize + "<" + min, shardSize >= min);
    }
    // The last shard includes the region beyond that the last scatter key
    int lastShardSize = (int) ((scatterKeys + 1) - lastId);
    assertTrue("Last shard has too many regions " + lastShardSize + ">" + max,
        lastShardSize <= max);
    assertTrue("Last shard has too few regions " + lastShardSize + "<" + min, lastShardSize >= min);
  }
}
