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
  private int nextSequenceNumber = 0;
  private int activeTaskCount;
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
    this.activeTaskCount = totalTaskCount;
    this.startTimeMillis = startTimeMillis;
    this.mostRecentUpdateTimeMillis = startTimeMillis;
    this.status = checkNotNull(status, "Null status");
    this.aggregateResult = initialAggregateResult;
  }

  @Override public String getJobId() {
    return jobId;
  }

  @Override public ShardedJobController<T, R> getController() {
    return controller;
  }

  @Override public ShardedJobSettings getSettings() {
    return settings;
  }

  @Override public int getTotalTaskCount() {
    return totalTaskCount;
  }

  @Override public long getStartTimeMillis() {
    return startTimeMillis;
  }

  @Override public long getMostRecentUpdateTimeMillis() {
    return mostRecentUpdateTimeMillis;
  }

  ShardedJobStateImpl<T, R> setMostRecentUpdateTimeMillis(long mostRecentUpdateTimeMillis) {
    this.mostRecentUpdateTimeMillis = mostRecentUpdateTimeMillis;
    return this;
  }

  long getNextSequenceNumber() {
    return nextSequenceNumber;
  }

  ShardedJobStateImpl<T, R> setNextSequenceNumber(int nextSequenceNumber) {
    this.nextSequenceNumber = nextSequenceNumber;
    return this;
  }

  @Override public int getActiveTaskCount() {
    return activeTaskCount;
  }

  ShardedJobStateImpl<T, R> setActiveTaskCount(int activeTaskCount) {
    this.activeTaskCount = activeTaskCount;
    return this;
  }

  @Override public Status getStatus() {
    return status;
  }

  ShardedJobStateImpl<T, R> setStatus(Status status) {
    this.status = checkNotNull(status, "Null status");
    return this;
  }

  @Override /*Nullable*/ public R getAggregateResult() {
    return aggregateResult;
  }

  ShardedJobStateImpl<T, R> setAggregateResult(/*Nullable*/ R aggregateResult) {
    this.aggregateResult = aggregateResult;
    return this;
  }

  @Override public String toString() {
    return getClass().getSimpleName() + "("
        + controller + ", "
        + nextSequenceNumber + ", "
        + status + ", "
        + activeTaskCount + "/" + totalTaskCount + ", "
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
    private static final String NEXT_SEQUENCE_NUMBER_PROPERTY = "nextSequenceNumber";
    private static final String ACTIVE_TASK_COUNT_PROPERTY = "activeTaskCount";
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
      out.setUnindexedProperty(NEXT_SEQUENCE_NUMBER_PROPERTY, in.getNextSequenceNumber());
      out.setUnindexedProperty(ACTIVE_TASK_COUNT_PROPERTY, in.getActiveTaskCount());
      out.setUnindexedProperty(STATUS_PROPERTY, "" + in.getStatus());
      if (in.getAggregateResult() != null) {
        out.setUnindexedProperty(AGGREGATE_RESULT_PROPERTY,
            new Blob(SerializationUtil.serializeToByteArray(in.getAggregateResult())));
      }
      return out;
    }

    @SuppressWarnings("unchecked")
    static <T extends IncrementalTask<T, R>, R extends Serializable>
          ShardedJobStateImpl<T, R> fromEntity(Entity in) {
      Preconditions.checkArgument(ENTITY_KIND.equals(in.getKind()), "Unexpected kind: %s", in);
      return new ShardedJobStateImpl<T, R>(in.getKey().getName(),
          (ShardedJobController<T, R>) SerializationUtil.deserializeFromDatastorePropertyUnchecked(
              in, CONTROLLER_PROPERTY),
          (ShardedJobSettings) SerializationUtil.deserializeFromDatastorePropertyUnchecked(
              in, SETTINGS_PROPERTY),
          Ints.checkedCast((Long) in.getProperty(TOTAL_TASK_COUNT_PROPERTY)),
          (Long) in.getProperty(START_TIME_PROPERTY),
          Status.valueOf((String) in.getProperty(STATUS_PROPERTY)),
          in.hasProperty(AGGREGATE_RESULT_PROPERTY) ? (R) SerializationUtil
              .deserializeFromDatastorePropertyUnchecked(in, AGGREGATE_RESULT_PROPERTY)
              : null)
          .setMostRecentUpdateTimeMillis((Long) in.getProperty(MOST_RECENT_UPDATE_TIME_PROPERTY))
          .setNextSequenceNumber(
              Ints.checkedCast((Long) in.getProperty(NEXT_SEQUENCE_NUMBER_PROPERTY)))
          .setActiveTaskCount(
              Ints.checkedCast((Long) in.getProperty(ACTIVE_TASK_COUNT_PROPERTY)));
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + activeTaskCount;
    result = prime * result + ((aggregateResult == null) ? 0 : aggregateResult.hashCode());
    result = prime * result + ((controller == null) ? 0 : controller.hashCode());
    result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
    result =
        prime * result + (int) (mostRecentUpdateTimeMillis ^ (mostRecentUpdateTimeMillis >>> 32));
    result = prime * result + nextSequenceNumber;
    result = prime * result + ((settings == null) ? 0 : settings.hashCode());
    result = prime * result + (int) (startTimeMillis ^ (startTimeMillis >>> 32));
    result = prime * result + ((status == null) ? 0 : status.hashCode());
    result = prime * result + totalTaskCount;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ShardedJobStateImpl<?, ?> other = (ShardedJobStateImpl<?, ?>) obj;
    if (activeTaskCount != other.activeTaskCount) {
      return false;
    }
    if (aggregateResult == null) {
      if (other.aggregateResult != null) {
        return false;
      }
    } else if (!aggregateResult.equals(other.aggregateResult)) {
      return false;
    }
    if (controller == null) {
      if (other.controller != null) {
        return false;
      }
    } else if (!controller.equals(other.controller)) {
      return false;
    }
    if (jobId == null) {
      if (other.jobId != null) {
        return false;
      }
    } else if (!jobId.equals(other.jobId)) {
      return false;
    }
    if (mostRecentUpdateTimeMillis != other.mostRecentUpdateTimeMillis) {
      return false;
    }
    if (nextSequenceNumber != other.nextSequenceNumber) {
      return false;
    }
    if (settings == null) {
      if (other.settings != null) {
        return false;
      }
    } else if (!settings.equals(other.settings)) {
      return false;
    }
    if (startTimeMillis != other.startTimeMillis) {
      return false;
    }
    if (status != other.status) {
      return false;
    }
    if (totalTaskCount != other.totalTaskCount) {
      return false;
    }
    return true;
  }

}
