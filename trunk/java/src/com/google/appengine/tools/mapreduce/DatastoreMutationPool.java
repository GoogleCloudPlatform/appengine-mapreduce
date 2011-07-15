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
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityTranslator;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * DatastoreMutationPool allows you to pool datastore operations such that
 * they are applied in batches requiring fewer datastore API calls. Mutations
 * are accumulated until they reach a count limit on the number of unflushed
 * mutations, until they reach a size limit on the byte count of unflushed
 * mutations, or until a manual flush is requested.
 *
 */
public class DatastoreMutationPool {
  /**
   * Default number of operations to batch before automatically flushing
   */
  public static final int DEFAULT_COUNT_LIMIT = 100;

  /**
   * Default size (in bytes) of operations to batch before automatically
   * flushing.
   *
   * <p>The current value is 256 KB.
   */
  public static final int DEFAULT_SIZE_LIMIT = 1 << 18;

  private DatastoreService ds;

  private int countLimit;
  private int sizeLimit;

  private List<Entity> puts = new ArrayList<Entity>();
  private int putsSize;

  private List<Key> deletes = new ArrayList<Key>();
  private int deletesSize;

  /**
   * Initialize a datastore mutation pool with the default batch mutation count
   * limit of {@value #DEFAULT_COUNT_LIMIT} and the default batch mutation
   * size limit of {@value #DEFAULT_SIZE_LIMIT}.
   */
  public DatastoreMutationPool(DatastoreService ds) {
    this(ds, DEFAULT_COUNT_LIMIT, DEFAULT_SIZE_LIMIT);
  }

  /**
   * Initialize a batch datastore mutation with the given count and size limits.
   */
  public DatastoreMutationPool(DatastoreService ds, int countLimit,
      int sizeLimit) {
    this.ds = ds;
    this.countLimit = countLimit;
    this.sizeLimit = sizeLimit;
  }

  /**
   * Adds a mutation inserting the given {@code entity}.
   */
  public void put(Entity entity) {
    int putSize = EntityTranslator.convertToPb(entity).getSerializedSize();

    // Do this before the add so that we guarantee that size is never > sizeLimit
    if (putsSize + putSize >= sizeLimit) {
      flushPuts();
    }

    putsSize += putSize;
    puts.add(entity);

    if (puts.size() >= countLimit) {
      flushPuts();
    }
  }

  private void flushPuts() {
    ds.put(puts);
    puts.clear();
    putsSize = 0;
  }

  /**
   * Adds a mutation deleting the entity corresponding to {@code key}.
   */
  public void delete(Key key) {
    // This is probably a serious overestimation, but I can't see a good
    // way to find the size in the public API.
    int deleteSize = KeyFactory.keyToString(key).length();

    // Do this before the add so that we guarantee that size is never > sizeLimit
    if (deletesSize + deleteSize >= sizeLimit) {
      flushDeletes();
    }

    deletesSize += deleteSize;
    deletes.add(key);

    if (deletes.size() >= countLimit) {
      flushDeletes();
    }
  }

  private void flushDeletes() {
    ds.delete(deletes);
    deletes.clear();
    deletesSize = 0;
  }

  /**
   * Flushes all outstanding mutations.
   */
  public void flush() {
    if (puts.size() > 0) {
      flushPuts();
    }

    if (deletes.size() > 0) {
      flushDeletes();
    }
  }
}
