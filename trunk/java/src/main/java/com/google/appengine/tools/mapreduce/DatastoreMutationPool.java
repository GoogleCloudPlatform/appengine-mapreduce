// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.Executors.callable;

import com.google.appengine.api.datastore.CommittedButStillApplyingException;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.DatastoreTimeoutException;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityTranslator;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.cloudstorage.ExceptionHandler;
import com.google.appengine.tools.cloudstorage.RetryHelper;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.google.apphosting.api.ApiProxy.ApiProxyException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Collection;
import java.util.ConcurrentModificationException;

/**
 * DatastoreMutationPool allows you to pool datastore operations such that they
 * are applied in batches, requiring fewer datastore API calls.  Mutations are
 * accumulated until they reach a count limit on the number of unflushed
 * mutations, until they reach a size limit on the byte count of unflushed
 * mutations, or until a manual flush is requested.
 *
 */
public class DatastoreMutationPool {

  public static final int DEFAULT_COUNT_LIMIT = 100;
  public static final int DEFAULT_BYTES_LIMIT = 256 * 1024;
  private static final ExceptionHandler EXCEPTION_HANDLER = new ExceptionHandler.Builder()
      .retryOn(ApiProxyException.class, ConcurrentModificationException.class,
          CommittedButStillApplyingException.class, DatastoreTimeoutException.class)
      .build();
  public static final Params DEFAULT_PARAMS = new Params.Builder().build();

  private final Params params;
  private final DatastoreService ds;
  private final Collection<Entity> puts = Lists.newArrayList();
  private int putsBytes;
  private final Collection<Key> deletes = Lists.newArrayList();
  private int deletesBytes;

  public static class Params implements Serializable {

    private static final long serialVersionUID = -4072996713626072011L;

    private final RetryParams retryParams;
    private final int countLimit;
    private final int bytesLimit;

    private Params(Builder builder) {
      retryParams = builder.retryParams;
      countLimit = builder.countLimit;
      bytesLimit = builder.bytesLimit;
    }

    public RetryParams getRetryParams() {
      return retryParams;
    }

    public int getCountLimit() {
      return countLimit;
    }

    public int getBytesLimit() {
      return bytesLimit;
    }

    public static class Builder {

      private int bytesLimit = DEFAULT_BYTES_LIMIT;
      private int countLimit = DEFAULT_COUNT_LIMIT;
      private RetryParams retryParams = RetryParams.getDefaultInstance();

      public Builder bytesLimit(int bytesLimit) {
        this.bytesLimit = bytesLimit;
        return this;
      }

      public Builder countLimit(int countLimit) {
        this.countLimit = countLimit;
        return this;
      }

      public Builder retryParams(RetryParams retryParams) {
        this.retryParams = checkNotNull(retryParams);
        return this;
      }

      public Params build() {
        return new Params(this);
      }
    }
  }

  private DatastoreMutationPool(DatastoreService ds, Params params) {
    this.ds = ds;
    this.params = params;
  }

  private static class Listener extends LifecycleListener {
    // Will never actually be serialized since it removes itself from the
    // registry on endSlice().  Declaring serialVersionUID only to avoid
    // compiler warning.
    private static final long serialVersionUID = 121172329066475329L;

    private DatastoreMutationPool pool;
    private final LifecycleListenerRegistry registry;

    Listener(DatastoreMutationPool pool, LifecycleListenerRegistry registry) {
      this.pool = checkNotNull(pool, "Null pool");
      this.registry = checkNotNull(registry, "Null registry");
    }

    @Override public void endSlice() {
      Preconditions.checkState(pool != null, "%s: endSlice() called twice?", this);
      registry.removeListener(this);
      try {
        pool.flush();
      } finally {
        pool = null;
      }
    }
  }

  public static DatastoreMutationPool forManualFlushing(DatastoreService ds, Params params) {
    return new DatastoreMutationPool(ds, params);
  }

  public static DatastoreMutationPool forManualFlushing(
      DatastoreService ds, int countLimit, int bytesLimit) {
    Params.Builder paramBuilder = new Params.Builder();
    paramBuilder.countLimit(countLimit);
    paramBuilder.bytesLimit(bytesLimit);
    return forManualFlushing(ds, paramBuilder.build());
  }

  public static DatastoreMutationPool forManualFlushing() {
    return forManualFlushing(
        DatastoreServiceFactory.getDatastoreService(), DEFAULT_COUNT_LIMIT, DEFAULT_BYTES_LIMIT);
  }

  /**
   * Adds a mutation to put the given entity to the datastore.
   */
  public void delete(Key key) {
    // This is probably a serious overestimation, but I can't see a good
    // way to find the size in the public API.
    int bytesHere = KeyFactory.keyToString(key).length();

    // Do this before the add so that we guarantee that size is never > sizeLimit
    if (deletesBytes + bytesHere >= params.getBytesLimit()) {
      flushDeletes();
    }

    deletesBytes += bytesHere;
    deletes.add(key);

    if (deletes.size() >= params.getCountLimit()) {
      flushDeletes();
    }
  }

  /**
   * Adds a mutation deleting the entity with the given key.
   */
  public void put(Entity entity) {
    int bytesHere = EntityTranslator.convertToPb(entity).getSerializedSize();

    // Do this before the add so that we guarantee that size is never > sizeLimit
    if (putsBytes + bytesHere >= params.getBytesLimit()) {
      flushPuts();
    }

    putsBytes += bytesHere;
    puts.add(entity);

    if (puts.size() >= params.getCountLimit()) {
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
    RetryHelper.runWithRetries(callable(new Runnable() {
      @Override public void run() {
        ds.delete(deletes);
        deletes.clear();
        deletesBytes = 0;
      }
    }), params.getRetryParams(), EXCEPTION_HANDLER);
  }

  private void flushPuts() {
    RetryHelper.runWithRetries(callable(new Runnable() {
      @Override public void run() {
        ds.put(puts);
        puts.clear();
        putsBytes = 0;
      }
    }), params.getRetryParams(), EXCEPTION_HANDLER);
  }
}
