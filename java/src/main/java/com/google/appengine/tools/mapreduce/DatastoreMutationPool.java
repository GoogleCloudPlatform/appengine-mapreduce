// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityTranslator;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;

/**
 * DatastoreMutationPool allows you to pool datastore operations such that they
 * are applied in batches, requiring fewer datastore API calls.  Mutations are
 * accumulated until they reach a count limit on the number of unflushed
 * mutations, until they reach a size limit on the byte count of unflushed
 * mutations, or until a manual flush is requested.
 *
 */
public class DatastoreMutationPool {

  private static final int DEFAULT_COUNT_LIMIT = 100;
  private static final int DEFAULT_BYTES_LIMIT = 256 * 1024;

  private final DatastoreService ds;
  private final int countLimit;
  private final int bytesLimit;

  private final Collection<Entity> puts = new ArrayList<Entity>();
  private int putsBytes = 0;

  private final Collection<Key> deletes = new ArrayList<Key>();
  private int deletesBytes = 0;

  // Private constructor to disallow subclasses (more flexible than making the class final).
  private DatastoreMutationPool(DatastoreService ds, int countLimit, int bytesLimit) {
    this.ds = ds;
    this.countLimit = countLimit;
    this.bytesLimit = bytesLimit;
  }

  private static class Listener extends LifecycleListener {
    // Will never actually be serialized since it removes itself from the
    // registry on endSlice().  Declaring serialVersionUID only to avoid
    // compiler warning.
    private static final long serialVersionUID = 121172329066475329L;

    private DatastoreMutationPool pool;
    private LifecycleListenerRegistry registry;

    Listener(DatastoreMutationPool pool, LifecycleListenerRegistry registry) {
      this.pool = checkNotNull(pool, "Null pool");
      this.registry = checkNotNull(registry, "Null registry");
    }

    @Override public void endSlice() {
      Preconditions.checkState(pool != null, "%s: endSlice() called twice?", this);
      registry.removeListener(this);
      DatastoreMutationPool p = pool;
      pool = null;
      p.flush();
    }
  }

  public static DatastoreMutationPool forManualFlushing(
      DatastoreService ds, int countLimit, int bytesLimit) {
    return new DatastoreMutationPool(ds, countLimit, bytesLimit);
  }

  public static DatastoreMutationPool forManualFlushing() {
    return forManualFlushing(
        DatastoreServiceFactory.getDatastoreService(), DEFAULT_COUNT_LIMIT, DEFAULT_BYTES_LIMIT);
  }

  public static DatastoreMutationPool forRegistry(
      LifecycleListenerRegistry registry, DatastoreService ds, int countLimit, int bytesLimit) {
    DatastoreMutationPool pool = forManualFlushing(ds, countLimit, bytesLimit);
    registry.addListener(new Listener(pool, registry));
    return pool;
  }

  public static DatastoreMutationPool forRegistry(LifecycleListenerRegistry registry) {
    return forRegistry(registry,
        DatastoreServiceFactory.getDatastoreService(), DEFAULT_COUNT_LIMIT, DEFAULT_BYTES_LIMIT);
  }

  public static DatastoreMutationPool forWorker(Worker<?> worker,
      DatastoreService ds, int countLimit, int bytesLimit) {
    return forRegistry(worker.getLifecycleListenerRegistry(), ds, countLimit, bytesLimit);
  }

  public static DatastoreMutationPool forWorker(Worker<?> worker) {
    return forRegistry(worker.getLifecycleListenerRegistry());
  }

  /**
   * Adds a mutation to put the given entity to the datastore.
   */
  public void delete(Key key) {
    // This is probably a serious overestimation, but I can't see a good
    // way to find the size in the public API.
    int bytesHere = KeyFactory.keyToString(key).length();

    // Do this before the add so that we guarantee that size is never > sizeLimit
    if (deletesBytes + bytesHere >= bytesLimit) {
      flushDeletes();
    }

    deletesBytes += bytesHere;
    deletes.add(key);

    if (deletes.size() >= countLimit) {
      flushDeletes();
    }
  }

  /**
   * Adds a mutation deleting the entity with the given key.
   */
  public void put(Entity entity) {
    int bytesHere = EntityTranslator.convertToPb(entity).getSerializedSize();

    // Do this before the add so that we guarantee that size is never > sizeLimit
    if (putsBytes + bytesHere >= bytesLimit) {
      flushPuts();
    }

    putsBytes += bytesHere;
    puts.add(entity);

    if (puts.size() >= countLimit) {
      flushPuts();
    }
  }

  /**
   * Performs all pending mutations.
   */
  public void flush() {
    if (!puts.isEmpty()) {
      flushPuts();
    }
    if (!deletes.isEmpty()) {
      flushDeletes();
    }
  }

  private void flushDeletes() {
    ds.delete(deletes);
    deletes.clear();
    deletesBytes = 0;
  }

  private void flushPuts() {
    ds.put(puts);
    puts.clear();
    putsBytes = 0;
  }

}
