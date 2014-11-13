// Copyright 2014 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;

/**
 * An input to read entity keys of a specified kind from the datastore.
 */
public final class DatastoreKeyInput extends BaseDatastoreInput<Key, DatastoreKeyInputReader> {

  private static final long serialVersionUID = -106587199386345409L;

  /**
   * @param entityKind entity kind to read from the datastore.
   * @param shardCount number of parallel shards for the input.
   */
  public DatastoreKeyInput(String entityKind, int shardCount) {
    this(entityKind, shardCount, null);
  }

  /**
   * @param entityKind entity kind to read from the datastore.
   * @param shardCount the number of parallel shards to divide the input into.
   * @param namespace the namespace of the entities (if null will use current).
   */
  public DatastoreKeyInput(String entityKind, int shardCount, String namespace) {
    this(createQuery(namespace, entityKind), shardCount);
  }

  /**
   * @param query The query to map read from the datastore
   * @param shardCount the number of parallel shards to divide the input into.
   */
  public DatastoreKeyInput(Query query, int shardCount) {
    super(query.setKeysOnly(), shardCount);
  }

  @Override
  protected DatastoreKeyInputReader createReader(Query query) {
    return new DatastoreKeyInputReader(query);
  }
}
