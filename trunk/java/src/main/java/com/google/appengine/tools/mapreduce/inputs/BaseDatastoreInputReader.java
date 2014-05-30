// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.inputs;

import static com.google.appengine.api.datastore.DatastoreServiceFactory.getDatastoreService;
import static com.google.appengine.api.datastore.Entity.KEY_RESERVED_PROPERTY;
import static com.google.appengine.api.datastore.FetchOptions.Builder.withChunkSize;
import static com.google.appengine.api.datastore.Query.FilterOperator.GREATER_THAN;
import static com.google.appengine.api.datastore.Query.FilterOperator.GREATER_THAN_OR_EQUAL;
import static com.google.appengine.api.datastore.Query.FilterOperator.LESS_THAN;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.CompositeFilterOperator;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.util.NoSuchElementException;

abstract class BaseDatastoreInputReader<V> extends InputReader<V> {

  private static final long serialVersionUID = -6080626196873436404L;
  private static final int BATCH_SIZE = 50;

  private final String entityKind;
  @VisibleForTesting final Key startKey;
  @VisibleForTesting final Key endKey;
  private final Function<Entity, V> transformFunc;
  private final String namespace;

  private Key currentKey;

  private transient QueryResultIterator<Entity> iterator;

  BaseDatastoreInputReader(String entityKind, Key startKey, /*Nullable*/ Key endKey,
      /*Nullable*/ String namespace, Function<Entity, V> transformFunc) {
    this.entityKind = checkNotNull(entityKind);
    this.startKey = checkNotNull(startKey);
    this.endKey = endKey;
    this.namespace = namespace;
    this.transformFunc = checkNotNull(transformFunc);
  }

  @Override
  public V next() {
    Preconditions.checkState(iterator != null, "%s: Not initialized: %s", this, iterator);
    if (!iterator.hasNext()) {
      throw new NoSuchElementException();
    }
    Entity entity = iterator.next();
    currentKey = entity.getKey();
    return transformFunc.apply(entity);
  }

  @Override
  public Double getProgress() {
    return null;
  }

  @Override
  public void beginShard() {
    currentKey = null;
    iterator = null;
  }

  @Override
  public void beginSlice() {
    Preconditions.checkState(iterator == null, "%s: Already initialized: %s", this, iterator);
    Query q = createQuery();
    iterator = getDatastoreService().prepare(q).asQueryResultIterator(withChunkSize(BATCH_SIZE));
  }

  @Override
  public long estimateMemoryRequirement() {
    return BATCH_SIZE * getAvgElementSize();
  }

  protected abstract long getAvgElementSize();

  protected Query createQuery() {
    Query q = BaseDatastoreInput.createQuery(entityKind, namespace).addSort(KEY_RESERVED_PROPERTY);
    Filter filter = currentKey == null ?
        new FilterPredicate(KEY_RESERVED_PROPERTY, GREATER_THAN_OR_EQUAL, startKey)
        : new FilterPredicate(KEY_RESERVED_PROPERTY, GREATER_THAN, currentKey);

    if (endKey != null) {
      filter = CompositeFilterOperator.and(
          filter, new FilterPredicate(KEY_RESERVED_PROPERTY, LESS_THAN, endKey));
    }
    q.setFilter(filter);
    return q;
  }
}
