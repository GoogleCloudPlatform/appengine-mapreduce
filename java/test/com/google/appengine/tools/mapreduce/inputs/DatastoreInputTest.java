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
import com.google.appengine.tools.mapreduce.InputReader;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 */
public class DatastoreInputTest extends TestCase {
// --------------------------- STATIC FIELDS ---------------------------

  static final String ENTITY_KIND_NAME = "Bob";

// ------------------------------ FIELDS ------------------------------

  private final LocalServiceTestHelper helper
      = new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());

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

  public void testCreateReadersLotsOfData() throws Exception {
    Collection<Entity> entities = new ArrayList<Entity>();
    for (int i = 0; i < 300; i++) {
      entities.add(new Entity(ENTITY_KIND_NAME));
    }

    List<Key> keys = ds.put(entities);
    Collections.sort(keys);

    List<? extends InputReader<Entity>> splits = createReaders(5);
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
    Collection<Entity> entities = new ArrayList<Entity>();
    for (int i = 0; i < 9; i++) {
      entities.add(new Entity(ENTITY_KIND_NAME));
    }

    List<Key> keys = ds.put(entities);

    List<? extends InputReader<Entity>> splits = createReaders(5);
    assertEquals(4, splits.size());
    assertStartAndEndKeys(splits.get(0), keys.get(0), keys.get(6));
    assertStartAndEndKeys(splits.get(1), keys.get(6), keys.get(7));
    assertStartAndEndKeys(splits.get(2), keys.get(7), keys.get(8));
    assertStartAndEndKeys(splits.get(3), keys.get(8), null);
  }

  public void testCreateReadersWithNoData() throws Exception {
    List<? extends InputReader<Entity>> splits = createReaders(10);
    assertEquals(0, splits.size());
  }

  public void testCreateReadersWithSingleKey() throws Exception {
    Key key = ds.put(new Entity(ENTITY_KIND_NAME));
    List<? extends InputReader<Entity>> splits = createReaders(1);

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

// -------------------------- STATIC METHODS --------------------------

  private static void assertStartAndEndKeys(InputReader<Entity> reader, Key startKey, Key endKey) {
    DatastoreInputReader datastoreInputSource = (DatastoreInputReader) reader;
    assertEquals("Start key doesn't match", startKey, datastoreInputSource.startKey);
    assertEquals("End key doesn't match", endKey, datastoreInputSource.endKey);
  }

  private static List<? extends InputReader<Entity>> createReaders(int shardCount) {
    DatastoreInput input = new DatastoreInput(ENTITY_KIND_NAME, shardCount);
    return input.createReaders();
  }

  private static void assertRegionSizeBounds(int scatterKeys, int numShards) {
    // Each shard should have the same number of regions, however since we round to the nearest
    // whole number the max size may be one larger than the min if the optimal number of regions
    // is a decimal value. The number of regions is 1 larger than the number of scatter keys.
    int min = (scatterKeys + 1) / numShards;
    int max = min + ((((scatterKeys + 1) % numShards) > 0) ? 1 : 0);

    ArrayList<Entity> entities = new ArrayList<Entity>(scatterKeys);
    for (int i = 1; i <= scatterKeys; i++) {
      entities.add(new Entity(ENTITY_KIND_NAME, i));
    }
    Iterable<Key> splitKeys = DatastoreInput.chooseSplitPoints(entities, numShards);
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
