// Copyright 2013 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.common.primitives.Ints;

/**
 * Retry information for a shard.
 *
 *
 * @param <T> type of task
 */
class ShardRetryState<T extends IncrementalTask> {

  private final String taskId;
  private final T initialTask;
  private int retryCount;

  private ShardRetryState(String taskId, T initialTask, int retryCount) {
    this.taskId = checkNotNull(taskId);
    this.initialTask = checkNotNull(initialTask);
    this.retryCount = retryCount;
  }

  public String getTaskId() {
    return taskId;
  }

  public T getInitialTask() {
    return initialTask;
  }

  public int getRetryCount() {
    return retryCount;
  }

  public int incrementAndGet() {
    return ++retryCount;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + taskId + ", " + retryCount + ", " + initialTask + ")";
  }

  static <T extends IncrementalTask> ShardRetryState<T> createFor(
      IncrementalTaskState<T> taskState) {
    return new ShardRetryState<>(taskState.getTaskId(), taskState.getTask(), 0);
  }

  // ShardRetryState should be using the same transactions as IncrementalTaskState
  static class Serializer {
    private static final String ENTITY_KIND = "MR-ShardRetryState";
    private static final String INITIAL_TASK_PROPERTY = "initialTask";
    private static final String RETRY_COUNT_PROPERTY = "retryCount";

    static Key makeKey(String taskId) {
      Key parent = IncrementalTaskState.Serializer.makeKey(taskId);
      return KeyFactory.createKey(parent, ENTITY_KIND, 1);
    }

    static Entity toEntity(ShardRetryState<?> in) {
      Entity shardInfo = new Entity(makeKey(in.getTaskId()));
      shardInfo.setUnindexedProperty(INITIAL_TASK_PROPERTY,
          new Blob(SerializationUtil.serializeToByteArray(in.initialTask)));
      shardInfo.setUnindexedProperty(RETRY_COUNT_PROPERTY, in.retryCount);
      return shardInfo;
    }

    static <T extends IncrementalTask> ShardRetryState<T> fromEntity(Entity in) {
      T initialTask = SerializationUtil.deserializeFromDatastoreProperty(in, INITIAL_TASK_PROPERTY);
      int retryCount = Ints.checkedCast((Long) in.getProperty(RETRY_COUNT_PROPERTY));
      return new ShardRetryState<>(in.getKey().getParent().getName(), initialTask, retryCount);
    }
  }
}
