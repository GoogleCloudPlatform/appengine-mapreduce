// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Query;

/**
 * An input to read entities of a specified kind from the datastore.
 *
 */
public final class DatastoreInput extends BaseDatastoreInput<Entity, DatastoreInputReader> {

  private static final long serialVersionUID = -106587199386345409L;

  /**
   * @param entityKind entity kind to read from the datastore.
   * @param shardCount number of parallel shards for the input.
   */
  public DatastoreInput(String entityKind, int shardCount) {
    this(entityKind, shardCount, null);
  }

  /**
   * @param entityKind entity kind to read from the datastore.
   * @param shardCount number of parallel shards for the input.
   * @param namespace the namespace of the entities (if null will use current).
   */
  public DatastoreInput(String entityKind, int shardCount, String namespace) {
    this(createQuery(namespace, entityKind), shardCount);
  }

  /**
   * @param query the query to read from the datastore.
   * @param shardCount the number for parallel shards for the input.
   */
  public DatastoreInput(Query query, int shardCount) {
    super(query, shardCount);
  }

  @Override
  protected DatastoreInputReader createReader(Query query) {
    return new DatastoreInputReader(query);
  }
}
