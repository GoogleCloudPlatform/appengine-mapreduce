// Copyright 2014 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.datastore.Key;

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
   * @param shardCount number of parallel shards for the input.
   * @param namespace the namespace of the entities (if null will use current).
   */
  public DatastoreKeyInput(String entityKind, int shardCount, String namespace) {
    super(entityKind, shardCount, namespace);
  }

  @Override
  protected DatastoreKeyInputReader createReader(
      String kind, Key start, Key end, String namespace) {
    return new DatastoreKeyInputReader(kind, start, end, namespace);
  }
}
