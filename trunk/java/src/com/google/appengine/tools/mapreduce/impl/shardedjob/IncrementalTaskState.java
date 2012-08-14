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
  private int nextSequenceNumber = 0;
  // If null, this task is finished.
  /*Nullable*/ private T nextTask;
  /*Nullable*/ private R partialResult;

  public IncrementalTaskState(String taskId,
      String jobId,
      long mostRecentUpdateMillis,
      T initialTask,
      R initialPartialResult) {
    this.taskId = checkNotNull(taskId, "Null taskId");
    this.jobId = checkNotNull(jobId, "Null jobId");
    this.mostRecentUpdateMillis = mostRecentUpdateMillis;
    this.nextTask = initialTask;
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

  @Override public String toString() {
    return getClass().getSimpleName() + "("
        + taskId + ", "
        + jobId + ", "
        + mostRecentUpdateMillis + ", "
        + nextSequenceNumber + ", "
        + nextTask + ", "
        + partialResult
        + ")";
  }

  static class Serializer {
    static final String ENTITY_KIND = "MR-IncrementalTask";

    private static final String JOB_ID_PROPERTY = "jobId";
    private static final String MOST_RECENT_UPDATE_MILLIS_PROPERTY = "mostRecentUpdateMillis";
    private static final String NEXT_SEQUENCE_NUMBER_PROPERTY = "sequenceNumber";
    private static final String NEXT_TASK_PROPERTY = "nextTask";
    private static final String PARTIAL_RESULT_PROPERTY = "partialResult";

    static Key makeKey(String taskId) {
      return KeyFactory.createKey(ENTITY_KIND, taskId);
    }

    static Entity toEntity(IncrementalTaskState in) {
      Entity out = new Entity(makeKey(in.getTaskId()));
      out.setProperty(JOB_ID_PROPERTY, in.getJobId());
      out.setUnindexedProperty(MOST_RECENT_UPDATE_MILLIS_PROPERTY, in.getMostRecentUpdateMillis());
      out.setProperty(NEXT_SEQUENCE_NUMBER_PROPERTY, in.getNextSequenceNumber());
      if (in.getNextTask() != null) {
        out.setUnindexedProperty(NEXT_TASK_PROPERTY,
            new Blob(SerializationUtil.serializeToByteArray(in.getNextTask())));
      }
      if (in.getPartialResult() != null) {
        out.setUnindexedProperty(PARTIAL_RESULT_PROPERTY,
            new Blob(SerializationUtil.serializeToByteArray(in.getPartialResult())));
      }
      return out;
    }

    @SuppressWarnings("unchecked")
    static <T extends IncrementalTask<T, R>, R extends Serializable>
        IncrementalTaskState<T, R> fromEntity(Entity in) {
      Preconditions.checkArgument(ENTITY_KIND.equals(in.getKind()), "Unexpected kind: %s", in);
      return new IncrementalTaskState(in.getKey().getName(),
          (String) in.getProperty(JOB_ID_PROPERTY),
          (Long) in.getProperty(MOST_RECENT_UPDATE_MILLIS_PROPERTY),
          in.hasProperty(NEXT_TASK_PROPERTY)
              ? (IncrementalTask) SerializationUtil.deserializeFromDatastorePropertyUnchecked(
                  in, NEXT_TASK_PROPERTY)
              : null,
          in.hasProperty(PARTIAL_RESULT_PROPERTY)
              ? SerializationUtil.deserializeFromDatastorePropertyUnchecked(
                  in, PARTIAL_RESULT_PROPERTY)
              : null)
          .setNextSequenceNumber(
              Ints.checkedCast((Long) in.getProperty(NEXT_SEQUENCE_NUMBER_PROPERTY)));
    }
  }

}
