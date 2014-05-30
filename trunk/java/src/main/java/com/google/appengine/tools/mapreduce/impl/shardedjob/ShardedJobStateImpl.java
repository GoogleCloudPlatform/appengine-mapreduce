// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import java.util.BitSet;

/**
 * Implements {@link ShardedJobState}, with additional package-private features.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <T> type of the IncrementalTask
 */
class ShardedJobStateImpl<T extends IncrementalTask> implements ShardedJobState {

  private final String jobId;
  private final ShardedJobController<T> controller;
  private final ShardedJobSettings settings;
  private final int totalTaskCount;
  private final long startTimeMillis;
  private long mostRecentUpdateTimeMillis;
  private BitSet shardsCompleted;
  private Status status;


  public static <T extends IncrementalTask> ShardedJobStateImpl<T> create(String jobId,
      ShardedJobController<T> controller, ShardedJobSettings settings, int totalTaskCount,
      long startTimeMillis) {
    return new ShardedJobStateImpl<>(checkNotNull(jobId, "Null jobId"),
        checkNotNull(controller, "Null controller"), checkNotNull(settings, "Null settings"),
        totalTaskCount, startTimeMillis, new Status(StatusCode.RUNNING));
  }

  private ShardedJobStateImpl(String jobId, ShardedJobController<T> controller,
      ShardedJobSettings settings, int totalTaskCount, long startTimeMillis, Status status) {
    this.jobId = jobId;
    this.controller = controller;
    this.settings = settings;
    this.totalTaskCount = totalTaskCount;
    this.shardsCompleted = new BitSet(totalTaskCount);
    this.startTimeMillis = startTimeMillis;
    this.mostRecentUpdateTimeMillis = startTimeMillis;
    this.status = status;
  }

  @Override
  public String getJobId() {
    return jobId;
  }

  @Override
  public String getJobName() {
    return controller == null ? "" : controller.getName();
  }

  ShardedJobController<T> getController() {
    return controller;
  }

  @Override
  public ShardedJobSettings getSettings() {
    return settings;
  }

  @Override
  public int getTotalTaskCount() {
    return totalTaskCount;
  }

  @Override
  public long getStartTimeMillis() {
    return startTimeMillis;
  }

  @Override
  public long getMostRecentUpdateTimeMillis() {
    return mostRecentUpdateTimeMillis;
  }

  @Override public int getActiveTaskCount() {
    return totalTaskCount - shardsCompleted.cardinality();
  }

  public void markShardCompleted(int shard) {
    shardsCompleted.set(shard);
  }

  private ShardedJobStateImpl<T> setShardsCompleted(BitSet shardsCompleted) {
    this.shardsCompleted = shardsCompleted;
    return this;
  }

  ShardedJobStateImpl<T> setMostRecentUpdateTimeMillis(long mostRecentUpdateTimeMillis) {
    this.mostRecentUpdateTimeMillis = mostRecentUpdateTimeMillis;
    return this;
  }

  @Override
  public Status getStatus() {
    return status;
  }

  ShardedJobStateImpl<T> setStatus(Status status) {
    this.status = checkNotNull(status, "Null status");
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "("
        + controller + ", "
        + status + ", "
        + shardsCompleted.cardinality() + "/" + totalTaskCount + ", "
        + mostRecentUpdateTimeMillis
        + ")";
  }

  static class ShardedJobSerializer {
    static final String ENTITY_KIND = "MR-ShardedJob";

    private static final String CONTROLLER_PROPERTY = "controller";
    private static final String START_TIME_PROPERTY = "startTimeMillis";
    private static final String SETTINGS_PROPERTY = "settings";
    private static final String TOTAL_TASK_COUNT_PROPERTY = "taskCount";
    private static final String MOST_RECENT_UPDATE_TIME_PROPERTY = "mostRecentUpdateTimeMillis";
    private static final String SHARDS_COMPLETED_PROPERTY = "activeShards";
    private static final String STATUS_PROPERTY = "status";

    static Key makeKey(String jobId) {
      return KeyFactory.createKey(ENTITY_KIND, jobId);
    }

    static Entity toEntity(ShardedJobStateImpl<?> in) {
      Entity out = new Entity(makeKey(in.getJobId()));
      out.setUnindexedProperty(CONTROLLER_PROPERTY,
          new Blob(SerializationUtil.serializeToByteArray(in.getController())));
      out.setUnindexedProperty(SETTINGS_PROPERTY,
          new Blob(SerializationUtil.serializeToByteArray(in.getSettings())));
      out.setUnindexedProperty(TOTAL_TASK_COUNT_PROPERTY, in.getTotalTaskCount());
      out.setUnindexedProperty(START_TIME_PROPERTY, in.getStartTimeMillis());
      out.setUnindexedProperty(MOST_RECENT_UPDATE_TIME_PROPERTY,
          in.getMostRecentUpdateTimeMillis());
      out.setUnindexedProperty(SHARDS_COMPLETED_PROPERTY,
          SerializationUtil.serializeToDatastoreProperty(in.shardsCompleted));
      out.setUnindexedProperty(STATUS_PROPERTY,
         SerializationUtil.serializeToDatastoreProperty(in.getStatus()));
      return out;
    }

    static <T extends IncrementalTask> ShardedJobStateImpl<T> fromEntity(Entity in) {
      return fromEntity(in, false);
    }

    static <T extends IncrementalTask> ShardedJobStateImpl<T> fromEntity(
        Entity in, boolean lenient) {
      Preconditions.checkArgument(ENTITY_KIND.equals(in.getKind()), "Unexpected kind: %s", in);
      return new ShardedJobStateImpl<>(in.getKey().getName(),
          SerializationUtil.<ShardedJobController<T>>deserializeFromDatastoreProperty(in,
              CONTROLLER_PROPERTY, lenient),
          SerializationUtil.<ShardedJobSettings>deserializeFromDatastoreProperty(
              in, SETTINGS_PROPERTY),
          Ints.checkedCast((Long) in.getProperty(TOTAL_TASK_COUNT_PROPERTY)),
          (Long) in.getProperty(START_TIME_PROPERTY),
          SerializationUtil.<Status>deserializeFromDatastoreProperty(in, STATUS_PROPERTY))
          .setMostRecentUpdateTimeMillis((Long) in.getProperty(MOST_RECENT_UPDATE_TIME_PROPERTY))
          .setShardsCompleted((BitSet) SerializationUtil.deserializeFromDatastoreProperty(in,
              SHARDS_COMPLETED_PROPERTY));
    }
  }
}
