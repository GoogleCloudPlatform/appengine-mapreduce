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

import static com.google.appengine.api.datastore.FetchOptions.Builder.withPrefetchSize;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.appengine.api.datastore.Text;
import com.google.common.base.Preconditions;

import com.googlecode.charts4j.AxisLabelsFactory;
import com.googlecode.charts4j.BarChart;
import com.googlecode.charts4j.Data;
import com.googlecode.charts4j.DataUtil;
import com.googlecode.charts4j.GCharts;
import com.googlecode.charts4j.Plot;
import com.googlecode.charts4j.Plots;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Wrapper for the MapReduceState entity that holds state for
 * the controller tasks.
 *
 *
 */
public class MapReduceState {

  // Property names
  public static final String ACTIVE_SHARD_COUNT_PROPERTY = "activeShardCount";
  public static final String CHART_PROPERTY = "chart";
  public static final String CONFIGURATION_PROPERTY = "configuration";
  public static final String COUNTERS_MAP_PROPERTY = "countersMap";
  public static final String LAST_POLL_TIME_PROPERTY = "lastPollTime";
  public static final String NAME_PROPERTY = "name";
  public static final String PROGRESS_PROPERTY = "progress";
  public static final String SHARD_COUNT_PROPERTY = "shardCount";
  public static final String START_TIME_PROPERTY = "startTime";
  public static final String STATUS_PROPERTY = "status";

  /**
   * Possible states of the status property
   */
  public static enum Status {
    ACTIVE,
    DONE
  }

  // DatastoreService to persist the state to
  private final DatastoreService service;

  // Wrapped entity
  private Entity entity;

  /**
   * Initialize MapReduceState with the given datastore and a {@code null} entity.
   */
  protected MapReduceState(DatastoreService service) {
    this(service, null);
  }

  /**
   * Initializes MapReduceState with the given datastore and entity.
   */
  protected MapReduceState(DatastoreService service, Entity entity) {
    this.service = service;
    this.entity = entity;
  }

  /**
   * Generates a MapReduceState that's configured with the given parameters, is
   * set as active, and has made no progress as of yet.
   *
   * The MapReduceState needs to have a configuration set via
   * {@code #setConfigurationXML(String)} before it can be persisted.
   *
   * @param service the datastore to persist the MapReduceState to
   * @string name user visible name for this MapReduce
   * @param jobId the JobID this MapReduceState corresponds to
   * @param time start time for this MapReduce, in milliseconds from the epoch
   * @return the initialized MapReduceState
   */
  public static MapReduceState generateInitializedMapReduceState(
      DatastoreService service, String name, JobID jobId, long time) {
    MapReduceState state = new MapReduceState(service);
    state.entity = new Entity("MapReduceState", jobId.toString());
    state.setName(name);
    state.entity.setProperty(PROGRESS_PROPERTY, 0.0);
    state.entity.setProperty(STATUS_PROPERTY, "" + Status.ACTIVE);
    state.entity.setProperty(START_TIME_PROPERTY, time);
    state.entity.setUnindexedProperty(CHART_PROPERTY, new Text(""));
    state.setCounters(new Counters());
    state.setActiveShardCount(0);
    state.setShardCount(0);
    return state;
  }

  /**
   * Gets the MapReduceState corresponding to the given job ID.
   *
   * @param service the datastore to use for persistence
   * @param jobId the JobID to retrieve the MapReduceState for
   * @return the corresponding MapReduceState
   * @throws EntityNotFoundException if there is no MapReduceState corresponding
   * to the given JobID
   */
  public static MapReduceState getMapReduceStateFromJobID(DatastoreService service,
      JobID jobId)
    throws EntityNotFoundException {
    Key key = KeyFactory.createKey("MapReduceState", jobId.toString());
    MapReduceState state = new MapReduceState(service);
    state.entity = service.get(key);
    return state;
  }

  /**
   * Gets a page of MapReduceStates.
   *
   * Given a cursor (possibly {@code null}) and a count, appends the page's
   * states to the {@code states} list, and returns a cursor for the next
   * page's position.
   */
  public static Cursor getMapReduceStates(
      DatastoreService service, String cursor, int count, List<MapReduceState> states) {
    FetchOptions fetchOptions = withPrefetchSize(count).limit(count);
    if (cursor != null) {
      fetchOptions = fetchOptions.startCursor(Cursor.fromWebSafeString(cursor));
    }
    QueryResultIterator<Entity> stateEntitiesIt = service.prepare(
        new Query("MapReduceState")).asQueryResultIterator(fetchOptions);

    while (stateEntitiesIt.hasNext()) {
      states.add(new MapReduceState(service, stateEntitiesIt.next()));
    }
    return stateEntitiesIt.getCursor();
  }

  /**
   * Set the progress estimate
   *
   * @param progress the current progress estimate
   */
  public void setProgress(double progress) {
    Preconditions.checkArgument(progress >= 0, "progress must be at least 0");
    Preconditions.checkArgument(progress <= 1, "progress must be at most 1");
    entity.setProperty(PROGRESS_PROPERTY, progress);
  }

  /**
   * Get the current progress estimate.
   *
   * @return the progress estimate
   */
  public double getProgress() {
    return (Double) entity.getProperty(PROGRESS_PROPERTY);
  }

  /**
   * Get the XML configuration used to start the MR.
   *
   * @return the configuration XML
   */
  public String getConfigurationXML() {
    return ((Text) entity.getProperty(CONFIGURATION_PROPERTY)).getValue();
  }

  /**
   * Set the configuration XML used to start the MR.
   *
   * @param configurationXML the configuration XML
   */
  public void setConfigurationXML(String configurationXML) {
    entity.setUnindexedProperty(CONFIGURATION_PROPERTY, new Text(configurationXML));
  }

  /**
   * Reconstitutes a Counters object from a MR state entity.
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

  private void checkComplete() {
    Preconditions.checkNotNull(getConfigurationXML(), "Configuration must be set.");
  }

  /**
   * Save the MapReduceState to the datastore.
   */
  public void persist() {
    checkComplete();
    service.put(entity);
  }

  /**
   * Sets the status to "done"
   */
  public void setDone() {
    entity.setProperty(STATUS_PROPERTY, "" + Status.DONE);
  }

  /**
   * Returns the current status: one of "active" or "done".
   *
   * @return the current status
   */
  public Status getStatus() {
    return Status.valueOf((String) entity.getProperty(STATUS_PROPERTY));
  }

  /**
   * Get the JobID for this MapReduceState.
   *
   * @return the JobID corresponding to this MapReduceState
   */
  public String getJobID() {
    return entity.getKey().getName();
  }

  /**
   * Returns the last time that we polled for quota updates.
   */
  public long getLastPollTime() {
    Long lastPollTime = (Long) entity.getProperty(LAST_POLL_TIME_PROPERTY);
    if (lastPollTime == null) {
      return -1;
    }
    return lastPollTime;
  }

  /**
   * Set the last poll time for future requests.
   *
   * @param time the time we last polled for quota updates in this request
   */
  public void setLastPollTime(long time) {
    entity.setProperty(LAST_POLL_TIME_PROPERTY, time);
  }

  /**
   * Returns the time this MR was started.
   */
  public long getStartTime() {
    return (Long) entity.getProperty(START_TIME_PROPERTY);
  }

  /**
   * Update this state to reflect the given set of mapper call counts.
   */
  public void setProcessedCounts(List<Long> processedCounts) {
    if (processedCounts == null || processedCounts.size() == 0) {
      return;
    }

    // If max == 0, the numeric range will be from 0 to 0. This causes some
    // problems when scaling to the range, so add 1 to max, assuming that the
    // smallest value can be 0, and this ensures that the chart always shows,
    // at a minimum, a range from 0 to 1 - when all shards are just starting.
    long maxPlusOne = Collections.max(processedCounts) + 1;

    List<String> countLabels = new ArrayList<String>();
    for (int i = 0; i < processedCounts.size(); i++) {
      countLabels.add(String.valueOf(i));
    }

    Data countData = DataUtil.scaleWithinRange(0, maxPlusOne, processedCounts);

    // TODO(user): Rather than returning charts from both servers, let's just
    // do it on the client's end.
    Plot countPlot = Plots.newBarChartPlot(countData);
    BarChart countChart = GCharts.newBarChart(countPlot);
    countChart.addYAxisLabels(AxisLabelsFactory.newNumericRangeAxisLabels(0, maxPlusOne));
    countChart.addXAxisLabels(AxisLabelsFactory.newAxisLabels(countLabels));
    countChart.setSize(300, 200);
    countChart.setBarWidth(BarChart.AUTO_RESIZE);
    countChart.setSpaceBetweenGroupsOfBars(1);
    entity.setUnindexedProperty(CHART_PROPERTY, new Text(countChart.toURLString()));
  }

  /**
   * Get the Google Charts URL for this MR's status chart.
   */
  public String getChartUrl() {
    return ((Text) entity.getProperty(CHART_PROPERTY)).getValue();
  }

  /**
   * Set a human readable name for this MapReduce.
   */
  public void setName(String name) {
    entity.setProperty(NAME_PROPERTY, name);
  }

  /**
   * Get the human readable name for this MapReduce.
   */
  public String getName() {
    return (String) entity.getProperty(NAME_PROPERTY);
  }

  /**
   * Get the shard count. This is the total number of shards ever in existence
   * concurrently.
   */
  public long getShardCount() {
    return (Long) entity.getProperty(SHARD_COUNT_PROPERTY);
  }

  /**
   * Set the shard count. Informative only - the real number is set
   * as a property in the MR's configuration.
   */
  public void setShardCount(long shardCount) {
    entity.setProperty(SHARD_COUNT_PROPERTY, shardCount);
  }

  /**
   * Get the number of shards currently active.
   */
  public long getActiveShardCount() {
    return (Long) entity.getProperty(ACTIVE_SHARD_COUNT_PROPERTY);
  }

  /**
   * Set the number of active shard. Informative only.
   */
  public void setActiveShardCount(long activeShardCount) {
    entity.setProperty(ACTIVE_SHARD_COUNT_PROPERTY, activeShardCount);
  }

  /**
   * Removes the underlying entity from the datastore. No other methods on
   * MapReduceState should be called after this one.
   */
  public void delete() {
    service.delete(entity.getKey());
  }

  /**
   * Create json object from this one. If detailed is true creates an object
   * with all the information needed for the job detail status view. Otherwise,
   * only includes the overview information.
   */
  public JSONObject toJson(boolean detailed) {
    JSONObject jobObject = new JSONObject();
    try {
      jobObject.put("name", getName());
      jobObject.put("mapreduce_id", getJobID().toString());
      jobObject.put("active", getStatus() == MapReduceState.Status.ACTIVE);
      jobObject.put("updated_timestamp_ms", getLastPollTime());
      jobObject.put("start_timestamp_ms", getStartTime());
      jobObject.put("result_status", "" + getStatus());

      if (detailed) {
        jobObject.put("counters", toJson(getCounters()));
        jobObject.put("configuration", getConfigurationXML());
        jobObject.put("chart_url", getChartUrl());

        // TODO(user): Fill this from the Configuration
        JSONObject mapperSpec = new JSONObject();
        mapperSpec.put("mapper_params", new JSONObject());
        jobObject.put("mapper_spec", mapperSpec);

        List<ShardState> shardStates = ShardState.getShardStatesFromJobID(service,
            JobID.forName(getJobID()));

        JSONArray shardArray = new JSONArray();
        for (ShardState shardState : shardStates) {
          shardArray.put(shardState.toJson());
        }
        jobObject.put("shards", shardArray);
      } else {
        jobObject.put("shards", getShardCount());
        jobObject.put("active_shards", getActiveShardCount());
      }
    } catch (JSONException e) {
      throw new RuntimeException("Hard coded string is null", e);
    }

    return jobObject;
  }

  private static JSONObject toJson(Counters counters) throws JSONException {
    JSONObject retValue = new JSONObject();
    for (CounterGroup group : counters) {
      for (Counter counter : group) {
        retValue.put(group.getName() + ":" + counter.getName(), counter.getValue());
      }
    }

    return retValue;
  }
}
