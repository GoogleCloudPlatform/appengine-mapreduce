// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.common.base.Function;

/**
 * An InputReader for Datastore entity keys.
 */
public class DatastoreKeyInputReader extends BaseDatastoreInputReader<Key> {

  private static final long serialVersionUID = 846982034548442467L;
  private static final long AVERAGE_KEY_SIZE = 256;

  private enum EntityToKeyFunction implements Function<Entity, Key> {
    INSTANCE;

    @Override
    public Key apply(Entity entity) {
      return entity.getKey();
    }
  }

  public DatastoreKeyInputReader(String entityKind, Key startKey, Key endKey, String namespace) {
    super(entityKind, startKey, endKey, namespace, EntityToKeyFunction.INSTANCE);
  }

  @Override
  protected Query createQuery() {
    return super.createQuery().setKeysOnly();
  }

  @Override
  protected long getAvgElementSize() {
    return AVERAGE_KEY_SIZE;
  }
}
