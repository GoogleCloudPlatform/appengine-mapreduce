// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.demos.mapreduce.entitycount;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.tools.mapreduce.DatastoreMutationPool;
import com.google.appengine.tools.mapreduce.Mapper;

import java.util.Random;
import java.util.logging.Logger;

/**
 * Creates random entities.
 *
 * @author ohler@google.com (Christian Ohler)
 */
class EntityCreator extends Mapper<Long, Void, Void> {
  private static final long serialVersionUID = 409204195454478863L;

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(EntityCreator.class.getName());

  private final String kind;
  private final int payloadBytesPerEntity;
  private final Random random = new Random();
  private transient DatastoreMutationPool pool;

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

  @Override public void beginShard() {
    pool = DatastoreMutationPool.forWorker(this);
  }

  @Override public void map(Long ignored) {
    String name = "" + (random.nextLong() & Long.MAX_VALUE);
    Entity e = new Entity(kind, name);
    // TODO(ohler): Verify that the datastore encodes text as 8 bits per
    // character, or use Blob instead.
    e.setProperty("payload", new Text(randomString(payloadBytesPerEntity)));
    pool.put(e);
  }

}
