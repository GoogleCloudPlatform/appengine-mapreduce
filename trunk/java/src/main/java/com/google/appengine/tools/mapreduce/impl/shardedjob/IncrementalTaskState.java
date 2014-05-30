// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.appengine.tools.mapreduce.impl.util.SerializationUtil.serializeToDatastoreProperty;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil.CompressionType;
import com.google.apphosting.api.ApiProxy;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

/**
 * Information about execution of an {@link IncrementalTask}.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <T> type of task
 */
public class IncrementalTaskState<T extends IncrementalTask> {

  private final String taskId;
  private final String jobId;
  private long mostRecentUpdateMillis;
  private int sequenceNumber;
  private int retryCount;
  private T task;
  private Status status;
  private LockInfo lockInfo;

  static class LockInfo {

    private static final String REQUEST_ID = "com.google.appengine.runtime.request_log_id";
    private Long startTime;
    private String requestId;

    private LockInfo(Long startTime, String requestId) {
      this.startTime = startTime;
      this.requestId = requestId;
    }

    public boolean isLocked() {
      return startTime != null;
    }

    public long lockedSince() {
      return startTime == null ? -1 : startTime;
    }

    public String getRequestId() {
      return requestId;
    }

    public void lock() {
      startTime = System.currentTimeMillis();
      requestId = (String) ApiProxy.getCurrentEnvironment().getAttributes().get(REQUEST_ID);
    }

    public void unlock() {
      startTime = null;
      requestId = null;
    }

    @Override
    public String toString() {
      return getClass().getName() + "(" + startTime + ", " + requestId + ")";
    }
  }

  /**
   * Returns a new running IncrementalTaskState.
   */
  static <T extends IncrementalTask> IncrementalTaskState<T> create(String taskId, String jobId,
      long createTime, T initialTask) {
    return new IncrementalTaskState<>(taskId, jobId, createTime, new LockInfo(null, null),
        checkNotNull(initialTask), new Status(StatusCode.RUNNING));
  }

  private IncrementalTaskState(String taskId, String jobId, long mostRecentUpdateMillis,
      LockInfo lockInfo, T task, Status status) {
    this.taskId = checkNotNull(taskId, "Null taskId");
    this.jobId = checkNotNull(jobId, "Null jobId");
    this.mostRecentUpdateMillis = mostRecentUpdateMillis;
    this.lockInfo = lockInfo;
    this.task = task;
    this.status = status;
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

  IncrementalTaskState<T> setMostRecentUpdateMillis(long mostRecentUpdateMillis) {
    this.mostRecentUpdateMillis = mostRecentUpdateMillis;
    return this;
  }

  public int getSequenceNumber() {
    return sequenceNumber;
  }

  IncrementalTaskState<T> setSequenceNumber(int nextSequenceNumber) {
    this.sequenceNumber = nextSequenceNumber;
    return this;
  }

  public int getRetryCount() {
    return retryCount;
  }

  int incrementAndGetRetryCount() {
    return ++retryCount;
  }

  void clearRetryCount() {
    retryCount = 0;
  }

  public LockInfo getLockInfo() {
    return lockInfo;
  }

  /*Nullable*/ public T getTask() {
    return task;
  }

  IncrementalTaskState<T> setTask(T task) {
    this.task = task;
    return this;
  }

  public Status getStatus() {
    return status;
  }

  public IncrementalTaskState<T> setStatus(Status status) {
    this.status = status;
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "("
        + taskId + ", "
        + jobId + ", "
        + mostRecentUpdateMillis + ", "
        + sequenceNumber + ", "
        + retryCount + ", "
        + task + ", "
        + status + ", "
        + ")";
  }

  static class Serializer {
    static final String ENTITY_KIND = "MR-IncrementalTask";
    static final String SHARD_INFO_ENTITY_KIND = ENTITY_KIND + "-ShardInfo";

    private static final String JOB_ID_PROPERTY = "jobId";
    private static final String MOST_RECENT_UPDATE_MILLIS_PROPERTY = "mostRecentUpdateMillis";
    private static final String SEQUENCE_NUMBER_PROPERTY = "sequenceNumber";
    private static final String RETRY_COUNT_PROPERTY = "retryCount";
    private static final String SLICE_START_TIME = "sliceStartTime";
    private static final String SLICE_REQUEST_ID = "sliceRequestId";
    private static final String NEXT_TASK_PROPERTY = "nextTask";
    private static final String STATUS_PROPERTY = "status";

    static Key makeKey(String taskId) {
      return KeyFactory.createKey(ENTITY_KIND, taskId);
    }

    static Entity toEntity(IncrementalTaskState<?> in) {
      Entity taskState = new Entity(makeKey(in.getTaskId()));
      taskState.setProperty(JOB_ID_PROPERTY, in.getJobId());
      taskState.setUnindexedProperty(MOST_RECENT_UPDATE_MILLIS_PROPERTY,
          in.getMostRecentUpdateMillis());
      if (in.getLockInfo().startTime != null) {
        taskState.setUnindexedProperty(SLICE_START_TIME, in.getLockInfo().startTime);
      }
      if (in.getLockInfo().requestId != null) {
        taskState.setUnindexedProperty(SLICE_REQUEST_ID, in.getLockInfo().requestId);
      }
      taskState.setProperty(SEQUENCE_NUMBER_PROPERTY, in.getSequenceNumber());
      taskState.setProperty(RETRY_COUNT_PROPERTY, in.getRetryCount());
      taskState.setUnindexedProperty(NEXT_TASK_PROPERTY,
          serializeToDatastoreProperty(in.getTask(), CompressionType.GZIP));
      taskState.setUnindexedProperty(STATUS_PROPERTY,
          SerializationUtil.serializeToDatastoreProperty(in.getStatus()));
      return taskState;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static <T extends IncrementalTask> IncrementalTaskState<T> fromEntity(Entity in) {
      Preconditions.checkArgument(ENTITY_KIND.equals(in.getKind()), "Unexpected kind: %s", in);
      IncrementalTaskState state = new IncrementalTaskState(in.getKey().getName(),
          (String) in.getProperty(JOB_ID_PROPERTY),
          (Long) in.getProperty(MOST_RECENT_UPDATE_MILLIS_PROPERTY),
          new LockInfo((Long) in.getProperty(SLICE_START_TIME),
              (String) in.getProperty(SLICE_REQUEST_ID)),
          in.hasProperty(NEXT_TASK_PROPERTY) ? SerializationUtil
              .<IncrementalTask>deserializeFromDatastoreProperty(in, NEXT_TASK_PROPERTY)
              : null,
          SerializationUtil.<Status>deserializeFromDatastoreProperty(in, STATUS_PROPERTY));
      state.setSequenceNumber(
          Ints.checkedCast((Long) in.getProperty(SEQUENCE_NUMBER_PROPERTY)));
      if (in.hasProperty(RETRY_COUNT_PROPERTY)) {
        state.retryCount = Ints.checkedCast((Long) in.getProperty(RETRY_COUNT_PROPERTY));
      }
      return state;
    }

    static boolean hasNextTask(Entity in) {
      Preconditions.checkArgument(ENTITY_KIND.equals(in.getKind()), "Unexpected kind: %s", in);
      return in.hasProperty(NEXT_TASK_PROPERTY);
    }
  }
}
