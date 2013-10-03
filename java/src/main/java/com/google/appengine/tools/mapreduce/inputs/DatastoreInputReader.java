// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.inputs;

import static com.google.appengine.api.datastore.DatastoreServiceFactory.getDatastoreService;
import static com.google.appengine.api.datastore.Entity.KEY_RESERVED_PROPERTY;
import static com.google.appengine.api.datastore.FetchOptions.Builder.withChunkSize;
import static com.google.appengine.api.datastore.Query.FilterOperator.GREATER_THAN;
import static com.google.appengine.api.datastore.Query.FilterOperator.GREATER_THAN_OR_EQUAL;
import static com.google.appengine.api.datastore.Query.FilterOperator.LESS_THAN;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.common.base.Preconditions;

import java.util.NoSuchElementException;

/**
 */
class DatastoreInputReader extends InputReader<Entity> {
// --------------------------- STATIC FIELDS ---------------------------

private static final long serialVersionUID = -2164845668646485549L;

// ------------------------------ FIELDS ------------------------------

  private final String entityKind;
  /*VisibleForTesting*/ final Key startKey;
  /*VisibleForTesting*/ final Key endKey;
  private final int batchSize = 50;
  private Key currentKey;

  private transient QueryResultIterator<Entity> iterator;

// --------------------------- CONSTRUCTORS ---------------------------

  DatastoreInputReader(String entityKind, Key startKey, Key endKey) {
    this.entityKind = entityKind;
    this.startKey = startKey;
    this.endKey = endKey;
  }

// ------------------------ METHODS ------------------------

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
  public void open() {
    currentKey = null;
  }

  @Override
  public void beginSlice() {
    Preconditions.checkState(iterator == null, "%s: Already initialized: %s", this, iterator);

    Query q = new Query(entityKind);
    Query.FilterPredicate firstFilter = null;

    if (currentKey == null) {
      if (startKey != null) {
        firstFilter =
            new Query.FilterPredicate(KEY_RESERVED_PROPERTY, GREATER_THAN_OR_EQUAL, startKey);
      }
    } else {
      firstFilter = new Query.FilterPredicate(KEY_RESERVED_PROPERTY, GREATER_THAN, currentKey);
    }

    if (endKey != null) {
      Query.FilterPredicate secondFilter =
          new Query.FilterPredicate(KEY_RESERVED_PROPERTY, LESS_THAN, endKey);
      q.setFilter(Query.CompositeFilterOperator.and(firstFilter, secondFilter));
    } else {
      q.setFilter(firstFilter);
    }

    q.addSort(KEY_RESERVED_PROPERTY);

    iterator = getDatastoreService().prepare(q).asQueryResultIterator(withChunkSize(batchSize));
  }
}
