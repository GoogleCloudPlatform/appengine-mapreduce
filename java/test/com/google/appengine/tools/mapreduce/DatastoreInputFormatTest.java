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
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
 * Tests the {@link DatastoreInputFormat} class.
 * 
 * @author frew@google.com (Fred Wulff)
 * 
 */
public class DatastoreInputFormatTest extends TestCase {
  private final LocalServiceTestHelper helper 
      = new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());
  
  private DatastoreService ds;
  
  final static String ENTITY_KIND_NAME = "Bob";
  
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
   * Inserts an entity with the given kind and name into the datastore.
   */
  public void putEntityWithName(String entityKind, String name) {
    Entity entity = new Entity(entityKind, name);
    ds.put(entity);
  }
    
  private void assertStartAndEndKeys(
      InputSplit split, Key startKey, Key endKey) {
    DatastoreInputSplit dsSplit = (DatastoreInputSplit) split;
    assertEquals(startKey, dsSplit.getStartKey());
    assertEquals(endKey, dsSplit.getEndKey());
  }
  
  /**
   * Asserts that split's start and keys have the given names.
   */
  private void assertStartAndEndKeyNames(
      InputSplit split, String entityKind, String startKeyName, String endKeyName) {
    Key startKey = KeyFactory.createKey(entityKind, startKeyName);
    Key endKey;
    if (endKeyName == null) {
      endKey = null;
    } else {
      endKey = KeyFactory.createKey(entityKind, endKeyName);
    }
    assertStartAndEndKeys(split, startKey, endKey);
  }
  
  /**
   * Create splits with sensible defaults for testing.
   */
  private List<InputSplit> getEntityKindSplitsFromCount(int shardCount) throws IOException {
    DatastoreInputFormat inputFormat = new DatastoreInputFormat();
    Configuration conf = new Configuration();
    conf.set(DatastoreInputFormat.ENTITY_KIND_KEY, ENTITY_KIND_NAME);
    conf.set(DatastoreInputFormat.SHARD_COUNT_KEY, "" + shardCount);

    JobContext context = new JobContext(conf, new JobID("Foo", 1));

    List<InputSplit> splits = inputFormat.getSplits(context);
    
    assertEquals(shardCount, splits.size());
    
    return splits;
  }
  
  public void testGetSplitWithNoData() throws Exception {
    DatastoreInputFormat inputFormat = new DatastoreInputFormat();
    Configuration conf = new Configuration();
    conf.set(DatastoreInputFormat.ENTITY_KIND_KEY, ENTITY_KIND_NAME);
    conf.set(DatastoreInputFormat.SHARD_COUNT_KEY, "1");

    JobContext context = new JobContext(conf, new JobID("Foo", 1));

    List<InputSplit> splits = inputFormat.getSplits(context);
    assertEquals(0, splits.size());
  }

  /**
   * Ensures that if the datastore has a single entity, then a split is 
   * generated for that entity.
   */
  public void testGetSplitsWithSingleKey() throws Exception {
    Key key = ds.put(new Entity(ENTITY_KIND_NAME));  
    List<InputSplit> splits = getEntityKindSplitsFromCount(1);
    
    assertStartAndEndKeys(splits.get(0), key, null);
  }
  
  /**
   * Test that input splits are generated correctly for datastore entities
   * that have keys with names.
   */
  public void testGetSplitsWithName() throws Exception {
    // Put three equally spaced entities, in reverse order - shouldn't matter
    // due to key sort.
    for (int i = 'c'; i >= 'a'; i--) {
      putEntityWithName(ENTITY_KIND_NAME, "" + (char) (i));
    }

    // TODO(frew): This test is only testing the single shard fallback
    // until the 1.4.2 release. At that point it should break and the expected
    // shards will need to be readjusted.
    List<InputSplit> splits = getEntityKindSplitsFromCount(1);
    assertStartAndEndKeyNames(splits.get(0), ENTITY_KIND_NAME, "a", null);
  }
  
  /**
   * Test that input splits are generated correctly for entities that have
   * keys without names.
   */
  // This test assumes that IDs are allocated sequentially.
  public void testGetSplitsWithId() throws Exception {

    List<Entity> entities = new ArrayList<Entity>();
    for (int i = 0; i < 3; i++) {
      entities.add(new Entity(ENTITY_KIND_NAME));
    }

    List<Key> keys = ds.put(entities);

    // TODO(frew): This test is only testing the single shard fallback
    // until the 1.4.2 release. At that point it should break and the expected
    // shards will need to be readjusted.
    List<InputSplit> splits = getEntityKindSplitsFromCount(1);
    assertStartAndEndKeys(splits.get(0), keys.get(0), null);
  }
}
