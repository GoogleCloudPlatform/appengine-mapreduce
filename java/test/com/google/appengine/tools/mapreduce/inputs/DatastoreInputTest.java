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

  public void testSplitLotsOfData() throws Exception {
    Collection<Entity> entities = new ArrayList<Entity>();
    for (int i = 0; i < 300; i++) {
      entities.add(new Entity(ENTITY_KIND_NAME));
    }

    List<Key> keys = ds.put(entities);
    Collections.sort(keys);

    List<? extends InputReader<Entity>> splits = split(5);
    assertEquals(5, splits.size());
    assertStartAndEndKeys(splits.get(0), keys.get(0), keys.get(62));
    assertStartAndEndKeys(splits.get(1), keys.get(62), keys.get(129));
    assertStartAndEndKeys(splits.get(2), keys.get(129), keys.get(194));
    assertStartAndEndKeys(splits.get(3), keys.get(194), keys.get(242));
    assertStartAndEndKeys(splits.get(4), keys.get(242), null);
  }

  public void testSplitNotEnoughData() throws Exception {
    Collection<Entity> entities = new ArrayList<Entity>();
    for (int i = 0; i < 10; i++) {
      entities.add(new Entity(ENTITY_KIND_NAME));
    }

    List<Key> keys = ds.put(entities);

    List<? extends InputReader<Entity>> splits = split(5);
    assertEquals(4, splits.size());
    assertStartAndEndKeys(splits.get(0), keys.get(0), keys.get(7));
    assertStartAndEndKeys(splits.get(1), keys.get(7), keys.get(8));
    assertStartAndEndKeys(splits.get(2), keys.get(8), keys.get(9));
    assertStartAndEndKeys(splits.get(3), keys.get(9), null);
  }

  public void testSplitWithNoData() throws Exception {
    List<? extends InputReader<Entity>> splits = split(10);
    assertEquals(0, splits.size());
  }

  public void testSplitWithSingleKey() throws Exception {
    Key key = ds.put(new Entity(ENTITY_KIND_NAME));
    List<? extends InputReader<Entity>> splits = split(1);

    assertEquals(1, splits.size());
    assertStartAndEndKeys(splits.get(0), key, null);
  }

// -------------------------- STATIC METHODS --------------------------

  private static void assertStartAndEndKeys(InputReader<Entity> reader, Key startKey, Key endKey) {
    DatastoreInputReader datastoreInputSource = (DatastoreInputReader) reader;
    assertEquals("Start key doesn't match", startKey, datastoreInputSource.startKey);
    assertEquals("End key doesn't match", endKey, datastoreInputSource.endKey);
  }

  private static List<? extends InputReader<Entity>> split(int shardCount) {
    DatastoreInput input = new DatastoreInput(ENTITY_KIND_NAME, shardCount);
    return input.createReaders();
  }
}
