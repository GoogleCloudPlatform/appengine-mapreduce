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

import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import junit.framework.TestCase;

/**
 * Tests the {@link AppEngineMapper} class.
 *
 */
public class AppEngineMapperTest extends TestCase {
  private static class TestMapper
      extends AppEngineMapper<NullWritable, NullWritable, NullWritable, NullWritable> {
    private boolean shouldOutput = false;

    @Override
    public void map(NullWritable key, NullWritable value, Context context) {
      if (shouldOutput) {
        Entity foo = new Entity("foo");
        ((AppEngineContext) context).getMutationPool().put(foo);
      }
    }

    public void shouldOutputEntity(boolean shouldOutput) {
      this.shouldOutput = shouldOutput;
    }
  }

  private final LocalServiceTestHelper helper
      = new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());

  private DatastoreService datastoreService;
  private TestMapper mapper;
  private TestMapper.AppEngineContext context;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    // datastoreService = DatastoreServiceFactory.getDatastoreService();
    mapper = new TestMapper();
    Configuration conf = new Configuration(false);
    TaskAttemptID id = new TaskAttemptID("foo", 1, true, 1, 1);
    context = mapper.new AppEngineContext(conf, id, null, null, null, null, null);
    datastoreService = DatastoreServiceFactory.getDatastoreService();
  }

  @Override
  protected void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

  /**
   * Just makes sure that things don't explode in the no mutation pool
   * case.
   */
  public void testNoMutationPool() throws Exception {
    mapper.setup(context);
    mapper.taskSetup(context);
    mapper.map(NullWritable.get(), NullWritable.get(), context);
    mapper.taskCleanup(context);
    mapper.cleanup(context);
  }

  public void testPartialMutationPool() throws Exception {
    mapper.shouldOutputEntity(true);
    mapper.setup(context);
    mapper.taskSetup(context);
    mapper.map(NullWritable.get(), NullWritable.get(), context);
    assertEquals(0, datastoreService.prepare(new Query("foo")).countEntities(FetchOptions.Builder.withDefaults()));
    mapper.taskCleanup(context);
    mapper.cleanup(context);
    assertEquals(1, datastoreService.prepare(new Query("foo")).countEntities(FetchOptions.Builder.withDefaults()));
  }

  public void testMutationPool() throws Exception {
    mapper.shouldOutputEntity(true);
    mapper.setup(context);
    mapper.taskSetup(context);
    for (int i = 0; i < DatastoreMutationPool.DEFAULT_COUNT_LIMIT - 1; i++) {
      mapper.map(NullWritable.get(), NullWritable.get(), context);
    }
    assertEquals(0, datastoreService.prepare(new Query("foo")).countEntities(FetchOptions.Builder.withDefaults()));
    mapper.map(NullWritable.get(), NullWritable.get(), context);
    assertEquals(DatastoreMutationPool.DEFAULT_COUNT_LIMIT,
                 datastoreService.prepare(new Query("foo")).countEntities(FetchOptions.Builder.withDefaults()));
    mapper.taskCleanup(context);
    mapper.cleanup(context);
    assertEquals(DatastoreMutationPool.DEFAULT_COUNT_LIMIT,
                 datastoreService.prepare(new Query("foo")).countEntities(FetchOptions.Builder.withDefaults()));
  }

  public void testMutationPoolFlushParameters() throws Exception {
    mapper.shouldOutputEntity(true);
    mapper.setup(context);
    mapper.taskSetup(context);
    context.setMutationPoolFlushParameters(10, 100000);
    for (int i = 0; i < 10 - 1; i++) {
      mapper.map(NullWritable.get(), NullWritable.get(), context);
    }
    assertEquals(0, datastoreService.prepare(new Query("foo")).countEntities(FetchOptions.Builder.withDefaults()));
    mapper.map(NullWritable.get(), NullWritable.get(), context);
    assertEquals(10,
                 datastoreService.prepare(new Query("foo")).countEntities(FetchOptions.Builder.withDefaults()));
    mapper.taskCleanup(context);
    mapper.cleanup(context);
    assertEquals(10,
                 datastoreService.prepare(new Query("foo")).countEntities(FetchOptions.Builder.withDefaults()));
  }

  public void testMutationPoolFlushParametersIllegalState() throws Exception {
    context.getMutationPool();
    try {
      context.setMutationPoolFlushParameters(10, 5);
      fail("Should have thrown IllegalStateException");
    } catch (IllegalStateException expected) {
    }
  }
}
