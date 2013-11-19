// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil.CompressionType;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import java.io.Serializable;

/**
 * Information about execution of an {@link IncrementalTask}.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <T> type of task
 * @param <R> type of intermediate and final results of the job
 */
class IncrementalTaskState<T extends IncrementalTask<T, R>, R extends Serializable> {

  private final String taskId;
  private final String jobId;
  private long mostRecentUpdateMillis;
  private int nextSequenceNumber;
  private int retryCount;
  // If null, this task is finished.
  /*Nullable*/ private T nextTask;
  /*Nullable*/ private R partialResult;

  static <T extends IncrementalTask<T, R>, R extends Serializable> IncrementalTaskState<T, R>
      create(String taskId, String jobId, long createTime, T initialTask, R initialResult) {
    IncrementalTaskState<T, R> taskState = new IncrementalTaskState<T, R>(
        taskId, jobId, createTime, checkNotNull(initialTask), checkNotNull(initialResult));
    return taskState;
  }

  private IncrementalTaskState(String taskId, String jobId, long mostRecentUpdateMillis,
      T nextTask, R initialPartialResult) {
    this.taskId = checkNotNull(taskId, "Null taskId");
    this.jobId = checkNotNull(jobId, "Null jobId");
    this.mostRecentUpdateMillis = mostRecentUpdateMillis;
    this.nextTask = nextTask;
    this.partialResult = initialPartialResult;
  }

  public String getTaskId() {
    return taskId;
  }

  public String getJobId() {
    return jobId;
  }

  public long getMostRecentUpdateMillis() {
    return mostRecentUpdateMillis;
  }

  public IncrementalTaskState<T, R> setMostRecentUpdateMillis(long mostRecentUpdateMillis) {
    this.mostRecentUpdateMillis = mostRecentUpdateMillis;
    return this;
  }

  public int getNextSequenceNumber() {
    return nextSequenceNumber;
  }

  public IncrementalTaskState<T, R> setNextSequenceNumber(int nextSequenceNumber) {
    this.nextSequenceNumber = nextSequenceNumber;
    return this;
  }

  public int getRetryCount() {
    return retryCount;
  }

  public int incrementAndGetRetryCount() {
    return ++retryCount;
  }

  public void clearRetryCount() {
    retryCount = 0;
  }

  /*Nullable*/ public T getNextTask() {
    return nextTask;
  }

  public IncrementalTaskState<T, R> setNextTask(/*Nullable*/ T nextTask) {
    this.nextTask = nextTask;
    return this;
  }

  /*Nullable*/ public R getPartialResult() {
    return partialResult;
  }

  public IncrementalTaskState<T, R> setPartialResult(/*Nullable*/ R partialResult) {
    this.partialResult = partialResult;
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "("
        + taskId + ", "
        + jobId + ", "
        + mostRecentUpdateMillis + ", "
        + nextSequenceNumber + ", "
        + retryCount + ", "
        + nextTask + ", "
        + partialResult + ")";
  }

  static class Serializer {
    static final String ENTITY_KIND = "MR-IncrementalTask";
    static final String SHARD_INFO_ENTITY_KIND = ENTITY_KIND + "-ShardInfo";

    private static final String JOB_ID_PROPERTY = "jobId";
    private static final String MOST_RECENT_UPDATE_MILLIS_PROPERTY = "mostRecentUpdateMillis";
    private static final String NEXT_SEQUENCE_NUMBER_PROPERTY = "sequenceNumber";
    private static final String RETRY_COUNT_PROPERTY = "retryCount";
    private static final String NEXT_TASK_PROPERTY = "nextTask";
    private static final String PARTIAL_RESULT_PROPERTY = "partialResult";

    static Key makeKey(String taskId) {
      return KeyFactory.createKey(ENTITY_KIND, taskId);
    }

    static Entity toEntity(IncrementalTaskState<?, ?> in) {
      Entity taskState = new Entity(makeKey(in.getTaskId()));
      taskState.setProperty(JOB_ID_PROPERTY, in.getJobId());
      taskState.setUnindexedProperty(
          MOST_RECENT_UPDATE_MILLIS_PROPERTY, in.getMostRecentUpdateMillis());
      taskState.setProperty(NEXT_SEQUENCE_NUMBER_PROPERTY, in.getNextSequenceNumber());
      taskState.setProperty(RETRY_COUNT_PROPERTY, in.getRetryCount());

      if (in.getNextTask() != null) {
        taskState.setUnindexedProperty(NEXT_TASK_PROPERTY,
            SerializationUtil.serializeToDatastoreProperty(in.getNextTask(), CompressionType.GZIP));
      }
      if (in.getPartialResult() != null) {
        taskState.setUnindexedProperty(PARTIAL_RESULT_PROPERTY,
            SerializationUtil.serializeToDatastoreProperty(
                in.getPartialResult(), CompressionType.GZIP));
      }
      return taskState;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static <T extends IncrementalTask<T, R>, R extends Serializable>
        IncrementalTaskState<T, R> fromEntity(Entity in) {
      Preconditions.checkArgument(ENTITY_KIND.equals(in.getKind()), "Unexpected kind: %s", in);
      IncrementalTaskState state = new IncrementalTaskState(
          in.getKey().getName(),
          (String) in.getProperty(JOB_ID_PROPERTY),
          (Long) in.getProperty(MOST_RECENT_UPDATE_MILLIS_PROPERTY),
          in.hasProperty(NEXT_TASK_PROPERTY)
              ? SerializationUtil.<IncrementalTask>deserializeFromDatastoreProperty(
                  in, NEXT_TASK_PROPERTY)
              : null,
          in.hasProperty(PARTIAL_RESULT_PROPERTY)
              ? SerializationUtil.deserializeFromDatastoreProperty(in, PARTIAL_RESULT_PROPERTY)
              : null)
          .setNextSequenceNumber(
              Ints.checkedCast((Long) in.getProperty(NEXT_SEQUENCE_NUMBER_PROPERTY)));
      if (in.hasProperty(RETRY_COUNT_PROPERTY)) {
        state.retryCount = Ints.checkedCast((Long) in.getProperty(RETRY_COUNT_PROPERTY));
      }
      return state;
    }

    static <R extends Serializable> R getPartialResult(Entity in) {
      Preconditions.checkArgument(ENTITY_KIND.equals(in.getKind()), "Unexpected kind: %s", in);
      if (!in.hasProperty(PARTIAL_RESULT_PROPERTY)) {
        return null;
      }
      return SerializationUtil.deserializeFromDatastoreProperty(in, PARTIAL_RESULT_PROPERTY);
    }

    static boolean hasNextTask(Entity in) {
      Preconditions.checkArgument(ENTITY_KIND.equals(in.getKind()), "Unexpected kind: %s", in);
      return in.hasProperty(NEXT_TASK_PROPERTY);
    }
  }
}
