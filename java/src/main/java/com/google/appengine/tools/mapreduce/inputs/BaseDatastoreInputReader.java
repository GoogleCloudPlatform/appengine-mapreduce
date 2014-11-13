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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;

abstract class BaseDatastoreInputReader<V> extends InputReader<V> {

  private static final long serialVersionUID = -2057811460496378627L;

  private static final int BATCH_SIZE = 50;

  private final Query query;
  private final Function<Entity, V> transformFunc;

  private Cursor cursor;

  private transient QueryResultIterator<Entity> iterator;

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
  }

  @Override

  public void beginSlice() {
    Preconditions.checkState(iterator == null, "%s: Already initialized: %s", this, iterator);
    FetchOptions options = withChunkSize(BATCH_SIZE);
    if (cursor != null) {
      options.startCursor(cursor);
    }
    iterator = getDatastoreService().prepare(query).asQueryResultIterator(options);
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
}
