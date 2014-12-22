// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.inputs;

import static com.google.appengine.api.datastore.DatastoreServiceFactory.getDatastoreService;
import static com.google.appengine.api.datastore.FetchOptions.Builder.withChunkSize;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Ticker;

abstract class BaseDatastoreInputReader<V> extends InputReader<V> {

  private static final long serialVersionUID = -2057811460496378627L;

  private static final int BATCH_SIZE = 50;

  private final Query query;
  private final Function<Entity, V> transformFunc;

  private Cursor cursor;
  private transient ResultIterator iterator;

  private static Ticker ticker = Ticker.systemTicker();

  private static class ResultIterator {

    private long startTimeNanos;
    private long accessTimeNanos;
    private Query query;
    private Cursor cursor;
    private QueryResultIterator<Entity> iterator;

    private static final long MAX_TIME_NANOS = 60_000_000_000L;
    private static final long MAX_IDLE_NANOS = 15_000_000_000L;

    ResultIterator(Query query, Cursor cursor) {
      this.query = query;
      this.cursor = cursor;
    }

    Cursor getCursor() {
      return iterator != null ? iterator.getCursor() : cursor;
    }

    Entity next() {
      long nowNanos = ticker.read();
      if (iterator == null || !isUsable(nowNanos)) {
        FetchOptions options = withChunkSize(BATCH_SIZE);
        Cursor cursor = getCursor();
        if (cursor != null) {
          options.startCursor(cursor);
        }
        iterator = getDatastoreService().prepare(query).asQueryResultIterator(options);
        startTimeNanos = nowNanos;
      }
      accessTimeNanos = nowNanos;
      // TODO(user): it would be nice if we could detect failures due to query expiration
      // and refresh the query when happens.
      return iterator.next();
    }

    boolean isUsable(long nowNanos) {
      return nowNanos - startTimeNanos < MAX_TIME_NANOS
          && nowNanos - accessTimeNanos < MAX_IDLE_NANOS;
    }
  }

  BaseDatastoreInputReader(Query query, Function<Entity, V> transformFunc) {
    this.query = checkNotNull(query);
    this.transformFunc = checkNotNull(transformFunc);
  }

  @Override
  public V next() {
    Preconditions.checkState(iterator != null, "%s: Not initialized: %s", this, iterator);
    return transformFunc.apply(iterator.next());
  }

  @Override
  public Double getProgress() {
    return null;
  }

  @Override
  public void beginShard() {
    iterator = null;
  }

  @Override
  public void endSlice() {
    cursor = iterator.getCursor();
    iterator = null;
  }

  @Override

  public void beginSlice() {
    Preconditions.checkState(iterator == null, "%s: Already initialized: %s", this, iterator);
    iterator = new ResultIterator(query, cursor);
  }

  @Override
  public long estimateMemoryRequirement() {
    return BATCH_SIZE * getAvgElementSize();
  }

  protected abstract long getAvgElementSize();

  @Override
  public String toString() {
    return getClass().getName() + " [query=" + query + ", transformFunc=" + transformFunc + "]";
  }

  @VisibleForTesting
  static void setTickerForTesting(Ticker ticker) {
    BaseDatastoreInputReader.ticker = ticker;
  }

  static void resetTicker() {
    BaseDatastoreInputReader.ticker = Ticker.systemTicker();
  }
}
