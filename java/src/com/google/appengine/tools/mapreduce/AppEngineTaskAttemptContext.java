/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.tools.mapreduce.v2.impl.ShardState;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

/**
 * This object is the analogue of {@link AppEngineJobContext} for the
 * per-task state.
 *
 * <p>Note: Our model of retries doesn't map very well to Hadoop's, so we
 * always say that we're on attempt 1. However, we still use TaskAttempt since
 * that's what the relevant Hadoop methods expect.
 *
 *
 */
public class AppEngineTaskAttemptContext extends TaskAttemptContext {
  // VisibleForTesting
  public static final String TASK_ATTEMPT_ID_PARAMETER_NAME = "taskAttemptID";

  private final ShardState state;

  /**
   * Gets a shard by its {@link TaskAttemptID}, throwing a {@link RuntimeException}
   * if the shard can't be found.
   */
  private static ShardState getShardStateFromTaskAttemptIdOrBlowUp(DatastoreService ds,
      TaskAttemptID attemptId) {
    try {
      return ShardState.getShardStateFromTaskAttemptId(ds, attemptId);
    } catch (EntityNotFoundException e) {
      throw new RuntimeException("Couldn't find shard state", e);
    }
  }

  /**
   * Initialize the context from a request, the jobContext created from the
   * request, and a datastore service to use for retrieving the ShardState.
   */
  public AppEngineTaskAttemptContext(HttpServletRequest req,
      AppEngineJobContext jobContext, DatastoreService ds) {
    // Same caveats about temporariness apply here as in JobContext.
    this(jobContext, getShardStateFromTaskAttemptIdOrBlowUp(ds, getTaskAttemptIDFromRequest(req)),
        getTaskAttemptIDFromRequest(req));
  }

  /**
   * Initialize the context from a task attempt ID, a context, and a shard state.
   */
  public AppEngineTaskAttemptContext(AppEngineJobContext jobContext, ShardState state,
      TaskAttemptID taskAttemptId) {
    super(jobContext.getConfiguration(), taskAttemptId);
    this.state = state;
  }

  /**
   * Gets the task attempt ID from the given request.
   *
   * @param req a servlet request with the job ID stored in the
   * {@link #TASK_ATTEMPT_ID_PARAMETER_NAME} parameter
   * @return the job ID
   */
  // VisibleForTesting
  static TaskAttemptID getTaskAttemptIDFromRequest(HttpServletRequest req) {
    String jobIdString = req.getParameter(TASK_ATTEMPT_ID_PARAMETER_NAME);
    if (jobIdString == null) {
      throw new RuntimeException("Couldn't get Job ID for request. Aborting!");
    }
    return TaskAttemptID.forName(jobIdString);
  }

  /**
   * Get the shard state for this task.
   */
  public ShardState getShardState() {
    return state;
  }

  /**
   * Reconstitutes the input split for this shard.
   *
   * @return the reconstituted shard
   */
  public InputSplit getInputSplit() {
    byte[] serializedSplit = state.getSerializedInputSplit();
    String splitClassName = state.getInputSplitClassName();
    return SerializationUtil.deserializeFromByteArray(
        getConfiguration(), InputSplit.class, splitClassName, serializedSplit, null);
  }

  /**
   * Reconstitutes the record reader for this invocation of this shard.
   *
   * @param <INKEY> the type of the key to be returned from the reader
   * @param <INVALUE> the type of the value to be returned from the reader
   * @param split the input split from {@link #getInputSplit()}
   * @return the reconstituted record reader
   * @throws IOException if there is an error reading the reader
   */
  @SuppressWarnings("unchecked")
  public <INKEY,INVALUE> RecordReader<INKEY, INVALUE> getRecordReader(InputSplit split) throws IOException {
    byte[] serializedReader = state.getSerializedRecordReader();
    String readerClassName = state.getRecordReaderClassName();
    try {
      RecordReader<INKEY,INVALUE> initReader =
        (RecordReader<INKEY,INVALUE>) ReflectionUtils.newInstance(
          getConfiguration().getClassByName(readerClassName),
          getConfiguration());
      initReader.initialize(split, this);
      return SerializationUtil.deserializeFromByteArray(
          getConfiguration(), RecordReader.class, readerClassName, serializedReader, initReader);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Got interrupted in a single threaded environment.", e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(
          "Couldn't find previously found RecordReader class: " + readerClassName, e);
    }
  }

  /**
   * Creates a {@link AppEngineMapper} for this task invocation.
   *
   * @param <INKEY> the type of the input keys for this mapper
   * @param <INVALUE> the type of the input values for this mapper
   * @param <OUTKEY> the type of the output keys for this mapper
   * @param <OUTVALUE> the type of the output values for this mapper
   * @return the new mapper
   */
  @SuppressWarnings("unchecked")
  public <INKEY,INVALUE,OUTKEY,OUTVALUE>
  AppEngineMapper<INKEY,INVALUE,OUTKEY,OUTVALUE> getMapper() {
    try {
      return (AppEngineMapper<INKEY,INVALUE,OUTKEY,OUTVALUE>)
      ReflectionUtils.newInstance(getMapperClass(),
        getConfiguration());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Couldn't find mapper class.", e);
    }
  }
}
