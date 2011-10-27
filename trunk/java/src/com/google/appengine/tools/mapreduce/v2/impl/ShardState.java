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

package com.google.appengine.tools.mapreduce.v2.impl;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.tools.mapreduce.SerializationUtil;
import com.google.appengine.tools.mapreduce.util.Clock;
import com.google.appengine.tools.mapreduce.util.SystemClock;
import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Thin wrapper around a shard state entity in the datastore.
 *
 *
 */
public class ShardState {
  // Property names in the shard state entity
  // VisibleForTesting
  static final String COUNTERS_MAP_PROPERTY = "countersMap";
  static final String INPUT_SPLIT_PROPERTY = "inputSplit";
  static final String INPUT_SPLIT_CLASS_PROPERTY = "inputSplitClass";
  static final String JOB_ID_PROPERTY = "jobId";
  static final String RECORD_READER_CLASS_PROPERTY = "recordReaderClass";
  static final String RECORD_READER_PROPERTY = "recordReader";
  static final String STATUS_STRING_PROPERTY = "statusString";
  static final String STATUS_PROPERTY = "status";
  static final String UPDATE_TIMESTAMP_PROPERTY = "updateTimestamp";

  /**
   * Possible states of the status property
   */
  public static enum Status {
    ACTIVE,
    DONE
  }

  // The datastore service to use for persisting the entity if updated.
  private final DatastoreService service;

  // The shard state entity
  private Entity entity;

  private Clock clock = new SystemClock();

  // VisibleForTesting
  public void setClock(Clock clock) {
    this.clock = clock;
  }

  /**
   * Creates the ShardState from its corresponding entity.
   *
   * @param service the datastore service to use for persisting the shard state
   */
  protected ShardState(DatastoreService service) {
    this.service = service;
  }

  /**
   * Gets the ShardState corresponding to the given TaskID.
   *
   * @param service the datastore to use for persisting the shard state
   * @param taskAttemptId the TaskID corresponding to this ShardState
   * @return the shard state corresponding to the provided key
   * @throws EntityNotFoundException if the given key can't be found
   */
  public static ShardState getShardStateFromTaskAttemptId(
      DatastoreService service, TaskAttemptID taskAttemptId) throws EntityNotFoundException {
    ShardState state = new ShardState(service);
    Key key = KeyFactory.createKey("ShardState", taskAttemptId.toString());
    state.entity = service.get(key);
    return state;
  }

  /**
   * Creates a shard state that's active but hasn't made any progress as of yet.
   *
   * The shard state isn't persisted when returned (so {@link #getKey()} will
   * return {@code null} until {@link #persist()} is called.
   *
   * @param service the datastore to persist the ShardState to
   * @param taskAttemptId the TaskAttemptID corresponding to the returned
   * ShardState
   * @return the initialized shard state
   */
  public static ShardState generateInitializedShardState(
      DatastoreService service, TaskAttemptID taskAttemptId) {
    ShardState shardState = new ShardState(service);

    shardState.entity = new Entity("ShardState", taskAttemptId.toString());
    shardState.entity.setProperty(JOB_ID_PROPERTY, taskAttemptId.getJobID().toString());

    Counters counters = new Counters();
    shardState.setCounters(counters);

    shardState.setStatusString("");
    shardState.entity.setProperty(STATUS_PROPERTY, "" + Status.ACTIVE);

    return shardState;
  }

  /**
   * Gets all shard states corresponding to a particular Job ID
   */
  public static List<ShardState> getShardStatesFromJobID(
      DatastoreService service, JobID jobId) {
    List<Entity> shardStateEntities = service.prepare(
        new Query("ShardState")
            .addFilter(JOB_ID_PROPERTY, FilterOperator.EQUAL, jobId.toString()))
            .asList(FetchOptions.Builder.withLimit(1000));
    List<ShardState> shardStates = new ArrayList<ShardState>(shardStateEntities.size());
    for (Entity entity : shardStateEntities) {
      ShardState shardState = new ShardState(service);
      shardState.entity = entity;
      shardStates.add(shardState);
    }
    return shardStates;
  }

  /**
   * Reconstitutes a Counters object from a shard state entity.
   * The returned counters is a copy. You must call
   * {@link #setCounters(Counters)} to persist updated counters to the
   * datastore.
   *
   * @return the reconstituted Counters object
   */
  public Counters getCounters() {
    Blob serializedMap = (Blob) entity.getProperty(COUNTERS_MAP_PROPERTY);
    Counters counters = new Counters();
    Writables.initializeWritableFromByteArray(serializedMap.getBytes(), counters);
    return counters;
  }

  /**
   * Saves counters to the datastore entity.
   *
   * @param counters the counters to serialize
   */
  public void setCounters(Counters counters) {
    entity.setUnindexedProperty(COUNTERS_MAP_PROPERTY,
        new Blob(Writables.createByteArrayFromWritable(counters)));
  }

  /**
   * Get the status string from the shard state. This is a user-defined
   * message, intended to inform a human of the status of the current shard.
   *
   * @return the status string
   */
  public String getStatusString() {
    return (String) entity.getProperty(STATUS_STRING_PROPERTY);
  }

  /**
   * Sets the status string for the shard state. This is a user-defined
   * message, intended to inform a human of the status of the current shard.
   *
   * @param status the status string
   */
  public void setStatusString(String status) {
    entity.setProperty(STATUS_STRING_PROPERTY, status);
  }

  /**
   * Get status. This is one of {@link Status}.
   *
   * @return the status as a string
   */
  public Status getStatus() {
    return Status.valueOf((String) entity.getProperty(STATUS_PROPERTY));
  }

  /**
   * Set the input split for this shard.
   *
   * @param conf the configuration to use for serializing the split
   * @param split the input split for this shard
   */
  public void setInputSplit(Configuration conf, InputSplit split) {
    Blob serializedSplit = new Blob(SerializationUtil.serializeToByteArray(conf, split));
    entity.setUnindexedProperty(INPUT_SPLIT_PROPERTY, serializedSplit);
    // TODO(user): Should we unify this with the Hadoop class stuff (or does it
    // matter?)
    entity.setProperty(INPUT_SPLIT_CLASS_PROPERTY, split.getClass().getCanonicalName());
  }

  /**
   * Get the input split for this shard.
   *
   * @return the serialized input split for this shard
   */
  public byte[] getSerializedInputSplit() {
    return ((Blob) entity.getProperty(INPUT_SPLIT_PROPERTY)).getBytes();
  }

  /**
   * Set the record reader for this shard.
   */
  public void setRecordReader(Configuration conf, RecordReader<?, ?> reader) {
    Blob serializedReader = new Blob(SerializationUtil.serializeToByteArray(conf, reader));
    entity.setUnindexedProperty(RECORD_READER_PROPERTY, serializedReader);
    entity.setProperty(RECORD_READER_CLASS_PROPERTY, reader.getClass().getCanonicalName());
  }

  /**
   * Get the record reader for this shard as a serialized byte array.
   */
  public byte[] getSerializedRecordReader() {
    return ((Blob) entity.getProperty(RECORD_READER_PROPERTY)).getBytes();
  }

  /**
   * Marks the current shard as done.
   */
  public void setDone() {
    entity.setProperty("status", Status.DONE.toString());
  }

  /**
   * Returns the canonical class name of the shard's input split.
   */
  public String getInputSplitClassName() {
    return (String) entity.getProperty(INPUT_SPLIT_CLASS_PROPERTY);
  }

  /**
   * Returns the class name of this shard's record reader.
   */
  public String getRecordReaderClassName() {
    return (String) entity.getProperty(RECORD_READER_CLASS_PROPERTY);
  }

  private void checkComplete() {
    Preconditions.checkNotNull(
        entity.getProperty(INPUT_SPLIT_PROPERTY),
        "Input split must be set.");
    Preconditions.checkNotNull(
        getInputSplitClassName(),
        "Input split must be set.");
    Preconditions.checkNotNull(
        entity.getProperty(RECORD_READER_PROPERTY),
        "Record reader must be set.");
    Preconditions.checkNotNull(
        getRecordReaderClassName(),
        "Record reader must be set.");
    Preconditions.checkNotNull(
        entity.getProperty(COUNTERS_MAP_PROPERTY),
        "Counters map must be set.");
  }

  /**
   * Persists this to the datastore.
   */
  public void persist() {
    checkComplete();
    setUpdateTimestamp(clock.currentTimeMillis());
    service.put(entity);
  }

  /**
   * Returns the update timestamp of this shard in milliseconds since the epoch.
   */
  public long getUpdateTimestamp() {
    return (Long) entity.getProperty(UPDATE_TIMESTAMP_PROPERTY);
  }

  /**
   * Set the time the state was last updated in milliseconds since the epoch.
   */
  public void setUpdateTimestamp(long timestamp) {
    entity.setProperty(UPDATE_TIMESTAMP_PROPERTY, timestamp);
  }

  /**
   * Gets the task attempt ID corresponding to this ShardState.
   * @return the task attempt ID corresponding to this ShardState
   */
  public TaskAttemptID getTaskAttemptID() {
    Preconditions.checkNotNull(entity.getKey().getName(),
        "ShardState must be persisted to call getTaskID()");
    return TaskAttemptID.forName(entity.getKey().getName());
  }

  /**
   * Gets the key for the underlying ShardState entity.
   */
  public Key getKey() {
    return entity.getKey();
  }

  /**
   * Create JSON object from this object.
   */
  public JSONObject toJson() {
    JSONObject shardObject = new JSONObject();
    try {
      shardObject.put("shard_number", getTaskAttemptID().getTaskID().getId());
      shardObject.put("active", getStatus() == ShardState.Status.ACTIVE);
      shardObject.put("shard_description", getStatusString());
      shardObject.put("updated_timestamp_ms", getUpdateTimestamp());
      shardObject.put("result_status", "" + getStatus());
    } catch (JSONException e) {
      throw new RuntimeException("Hard coded string is null", e);
    }
    return shardObject;
  }
}
