// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;

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
    super(new ScatterDatastoreShardStrategy(entityKind, namespace), entityKind, shardCount,
        namespace);
  }

  @Override
  protected DatastoreInputReader createReader(String kind, Key start, Key end, String namespace) {
    return new DatastoreInputReader(kind, start, end, namespace);
  }
}
