// Copyright 2011 Google Inc. All Rights Reserved.

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
import com.google.common.base.Preconditions;

import java.util.NoSuchElementException;

/**
 */
class DatastoreInputReader extends InputReader<Entity> {

  private static final long serialVersionUID = -2164845668646485549L;
  private static final long AVERAGE_ENTITY_SIZE = 100 * 1024;
  private static final int BATCH_SIZE = 50;

  private final String entityKind;
  @VisibleForTesting final Key startKey;
  @VisibleForTesting final Key endKey;

  private Key currentKey;
  private final String namespace;

  private transient QueryResultIterator<Entity> iterator;

  DatastoreInputReader(
      String entityKind, Key startKey, /*Nullable*/ Key endKey, /*Nullable*/ String namespace) {
    this.entityKind = checkNotNull(entityKind);
    this.startKey = checkNotNull(startKey);
    this.endKey = endKey;
    this.namespace = namespace;
  }

  @Override
  public Entity next() {
    Preconditions.checkState(iterator != null, "%s: Not initialized: %s", this, iterator);
    if (!iterator.hasNext()) {
      throw new NoSuchElementException();
    }
    Entity entity = iterator.next();
    currentKey = entity.getKey();
    return entity;
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

    Query q = DatastoreInput.createQuery(entityKind, namespace).addSort(KEY_RESERVED_PROPERTY);
    Filter filter = currentKey == null ?
        new FilterPredicate(KEY_RESERVED_PROPERTY, GREATER_THAN_OR_EQUAL, startKey)
        : new FilterPredicate(KEY_RESERVED_PROPERTY, GREATER_THAN, currentKey);

    if (endKey != null) {
      filter = CompositeFilterOperator.and(
          filter, new FilterPredicate(KEY_RESERVED_PROPERTY, LESS_THAN, endKey));
    }
    q.setFilter(filter);
    iterator = getDatastoreService().prepare(q).asQueryResultIterator(withChunkSize(BATCH_SIZE));
  }

  @Override
  public long estimateMemoryRequirement() {
    return BATCH_SIZE * AVERAGE_ENTITY_SIZE;
  }
}
