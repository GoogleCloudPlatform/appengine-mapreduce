// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import java.io.Serializable;
import java.util.BitSet;

/**
 * Implements {@link ShardedJobState}, with additional package-private features.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <T> type of tasks that the job consists of
 * @param <R> type of intermediate and final results of the job
 */
class ShardedJobStateImpl<T extends IncrementalTask<T, R>, R extends Serializable>
    implements ShardedJobState<T, R> {

  private final String jobId;
  private final ShardedJobController<T, R> controller;
  private final ShardedJobSettings settings;
  private final int totalTaskCount;
  private final long startTimeMillis;
  private long mostRecentUpdateTimeMillis;
  private BitSet shardsCompleted;
  private Status status;
  /*Nullable*/ private R aggregateResult;

  public ShardedJobStateImpl(String jobId,
      ShardedJobController<T, R> controller,
      ShardedJobSettings settings,
      int totalTaskCount,
      long startTimeMillis,
      Status status,
      R initialAggregateResult) {
    this.jobId = checkNotNull(jobId, "Null jobId");
    this.controller = checkNotNull(controller, "Null controller");
    this.settings = checkNotNull(settings, "Null settings");
    this.totalTaskCount = totalTaskCount;
    this.shardsCompleted = new BitSet(totalTaskCount);
    this.startTimeMillis = startTimeMillis;
    this.mostRecentUpdateTimeMillis = startTimeMillis;
    this.status = checkNotNull(status, "Null status");
    this.aggregateResult = initialAggregateResult;
  }

  @Override
  public String getJobId() {
    return jobId;
  }

  @Override
  public ShardedJobController<T, R> getController() {
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

  private ShardedJobStateImpl<T, R> setShardsCompleted(BitSet shardsCompleted) {
    this.shardsCompleted = shardsCompleted;
    return this;
  }
  
  ShardedJobStateImpl<T, R> setMostRecentUpdateTimeMillis(long mostRecentUpdateTimeMillis) {
    this.mostRecentUpdateTimeMillis = mostRecentUpdateTimeMillis;
    return this;
  }

  @Override
  public Status getStatus() {
    return status;
  }

  ShardedJobStateImpl<T, R> setStatus(Status status) {
    this.status = checkNotNull(status, "Null status");
    return this;
  }

  @Override
  /*Nullable*/ public R getAggregateResult() {
    return aggregateResult;
  }

  ShardedJobStateImpl<T, R> setAggregateResult(/*Nullable*/ R aggregateResult) {
    this.aggregateResult = aggregateResult;
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "("
        + controller + ", "
        + status + ", "
        + shardsCompleted.cardinality() + "/" + totalTaskCount + ", "
        + mostRecentUpdateTimeMillis + ", "
        + aggregateResult
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
    private static final String AGGREGATE_RESULT_PROPERTY = "result";

    static Key makeKey(String jobId) {
      return KeyFactory.createKey(ENTITY_KIND, jobId);
    }

    static Entity toEntity(ShardedJobStateImpl<?, ?> in) {
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
          new Blob(SerializationUtil.serializeToByteArray(in.shardsCompleted)));
      out.setUnindexedProperty(STATUS_PROPERTY,
          new Blob(SerializationUtil.serializeToByteArray(in.getStatus())));
      if (in.getAggregateResult() != null) {
        out.setUnindexedProperty(AGGREGATE_RESULT_PROPERTY,
            new Blob(SerializationUtil.serializeToByteArray(in.getAggregateResult())));
      }
      return out;
    }

    static <T extends IncrementalTask<T, R>,
        R extends Serializable> ShardedJobStateImpl<T, R> fromEntity(Entity in) {
      Preconditions.checkArgument(ENTITY_KIND.equals(in.getKind()), "Unexpected kind: %s", in);
      return new ShardedJobStateImpl<T, R>(
          in.getKey().getName(),
          SerializationUtil.<ShardedJobController<T, R>>deserializeFromDatastoreProperty(
              in, CONTROLLER_PROPERTY),
          SerializationUtil.<ShardedJobSettings>deserializeFromDatastoreProperty(
              in, SETTINGS_PROPERTY),
          Ints.checkedCast((Long) in.getProperty(TOTAL_TASK_COUNT_PROPERTY)),
          (Long) in.getProperty(START_TIME_PROPERTY),
          SerializationUtil.<Status>deserializeFromDatastoreProperty(in, STATUS_PROPERTY),
          in.hasProperty(AGGREGATE_RESULT_PROPERTY) ?
              SerializationUtil.<R>deserializeFromDatastoreProperty(in, AGGREGATE_RESULT_PROPERTY)
              : null).setMostRecentUpdateTimeMillis(
          (Long) in.getProperty(MOST_RECENT_UPDATE_TIME_PROPERTY)).setShardsCompleted(
          (BitSet) SerializationUtil.deserializeFromDatastoreProperty(in,
              SHARDS_COMPLETED_PROPERTY));
    }
  }

}
