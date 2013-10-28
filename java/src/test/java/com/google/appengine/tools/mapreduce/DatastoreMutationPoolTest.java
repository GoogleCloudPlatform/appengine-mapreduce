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
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.EntityTranslator;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

import junit.framework.TestCase;

/**
 * Tests {@link DatastoreMutationPool}.
 *
 */
public class DatastoreMutationPoolTest extends TestCase {
// ------------------------------ FIELDS ------------------------------

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());

  private DatastoreService ds;
  private Entity[] entities;

// ------------------------ OVERRIDING METHODS ------------------------

  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    ds = DatastoreServiceFactory.getDatastoreService();
    entities = new Entity[]{new Entity("Foo"), new Entity("Foo"), new Entity("Foo")};
  }

  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

// -------------------------- TEST METHODS --------------------------

  public void testCountFlush() {
    DatastoreMutationPool pool = makeMutationPool(2, 1 << 18);
    // Check last put so we can isolate problems with manual flush() from problems with
    // put.
    checkFlushOnNthPutOutOfThree(pool, 1, 0);
    pool.flush();
    checkFlushOnNthDeleteOutOfThree(pool, 1, 0);
  }

  public void testCountFlush_allAutomatic() {
    DatastoreMutationPool pool = makeMutationPool(3, 1 << 18);
    // Check last put so we can isolate problems with manual flush() from problems with
    // put.
    checkFlushOnNthPutOutOfThree(pool, 2, 0);
    checkFlushOnNthDeleteOutOfThree(pool, 2, 0);
  }

  public void testManualFlush() {
    DatastoreMutationPool pool = makeMutationPool(1000, 1000);
    pool.put(entities[0]);
    pool.flush();
    try {
      ds.get(entities[0].getKey());
    } catch (EntityNotFoundException e) {
      fail("Put wasn't flushed when expected.");
    }

    pool.delete(entities[0].getKey());
    pool.flush();

    try {
      ds.get(entities[0].getKey());
      fail("Delete wasn't flushed when expected.");
    } catch (EntityNotFoundException expected) {
    }
  }

  public void testSizeFlush() {
    DatastoreMutationPool putPool = makeMutationPool(1000,
        EntityTranslator.convertToPb(entities[0]).getSerializedSize() + 1);
    checkFlushOnNthPutOutOfThree(putPool, 1, -1);
    putPool.flush();
    DatastoreMutationPool deletePool = makeMutationPool(1000,
        KeyFactory.keyToString(entities[0].getKey()).length() + 1);
    checkFlushOnNthDeleteOutOfThree(deletePool, 1, -1);
  }

// -------------------------- INSTANCE METHODS --------------------------

  private DatastoreMutationPool makeMutationPool(int countLimit, int bytesLimit) {
    return DatastoreMutationPool.forManualFlushing(ds, countLimit, bytesLimit);
  }

  /**
   * Attempts to add and then remove three entities, asserting that the
   * mutation pool is flushed when the nth entity is added.
   */
  private void checkFlushOnNthDeleteOutOfThree(
      DatastoreMutationPool pool, int n, int offsetToCheck) {
    int i;
    for (i = 0; i < n; i++) {
      pool.delete(entities[i].getKey());
      try {
        ds.get(entities[i].getKey());
      } catch (EntityNotFoundException e) {
        fail("Deletes were flushed prematurely.");
      }
    }

    pool.delete(entities[i].getKey());
    try {
      ds.get(entities[i + offsetToCheck].getKey());
      fail("Deletes didn't get flushed on cue.");
    } catch (EntityNotFoundException expected) {
    }

    i++;

    for (; i < entities.length; i++) {
      pool.delete(entities[i].getKey());
      try {
        ds.get(entities[i].getKey());
      } catch (EntityNotFoundException e) {
        fail("Deletes got flushed prematurely.");
      }
    }
  }

  /**
   * Attempts to add and then remove three entities, asserting that the
   * mutation pool is flushed when the nth entity is added.
   */
  private void checkFlushOnNthPutOutOfThree(
      DatastoreMutationPool pool, int n, int offsetToCheck) {
    int i;
    for (i = 0; i < n; i++) {
      pool.put(entities[i]);
      try {
        ds.get(entities[i].getKey());
        fail("Entity got flushed prematurely.");
      // Note: either of the exceptions are fine: EntityNotFound is
      // self-explanatory. IllegalArgument is just complaining that the entity's
      // key isn't complete because no ID has been assigned it yet.
      } catch (EntityNotFoundException expected) {
      } catch (IllegalArgumentException expected) {
      }
    }

    pool.put(entities[i]);
    try {
      ds.get(entities[i + offsetToCheck].getKey());
    } catch (EntityNotFoundException e) {
      fail("Entities didn't get flushed on cue.");
    }

    i++;

    for (; i < entities.length; i++) {
      pool.put(entities[i]);
      try {
        ds.get(entities[i].getKey());
        fail("Entity got flushed prematurely.");
      // See above for explanation
      } catch (EntityNotFoundException expected) {
      } catch (IllegalArgumentException expected) {
      }
    }
  }
}
