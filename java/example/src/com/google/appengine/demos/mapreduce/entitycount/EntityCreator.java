// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.demos.mapreduce.entitycount;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.tools.mapreduce.DatastoreMutationPool;
import com.google.appengine.tools.mapreduce.Mapper;

import java.util.Random;

/**
 * Creates random entities.
 *
 * @author ohler@google.com (Christian Ohler)
 */
class EntityCreator extends Mapper<Long, Void, Void> {

  private static final long serialVersionUID = 409204195454478863L;

  private final String kind;
  private final int payloadBytesPerEntity;
  private final Random random = new Random();
  // [START datastoreMutationPool]
  private transient DatastoreMutationPool pool;
  // [END datastoreMutationPool]

  public EntityCreator(String kind, int payloadBytesPerEntity) {
    this.kind = checkNotNull(kind, "Null kind");
    this.payloadBytesPerEntity = payloadBytesPerEntity;
  }

  private String randomString(int length) {
    StringBuilder out = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      out.append((char) ('a' + random.nextInt(26)));
    }
    return out.toString();
  }

  // [START begin_and_endSlice]
  @Override
  public void beginSlice() {
    pool = DatastoreMutationPool.create();
  }

  @Override
  public void endSlice() {
    pool.flush();
  }
  // [END begin_and_endSlice]

  @Override
  public void map(Long ignored) {
    String name = String.valueOf(random.nextLong() & Long.MAX_VALUE);
    Entity entity = new Entity(kind, name);
    entity.setProperty("payload", new Text(randomString(payloadBytesPerEntity)));
    pool.put(entity);
  }
}
