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

import com.google.appengine.tools.mapreduce.v2.impl.ShardState;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.StatusReporter;

import java.util.logging.Logger;


/**
 * A Hadoop StatusReporter that saves reported data to a {@link ShardState}.
 *
 */
public class DatastorePersistingStatusReporter extends StatusReporter {
  private static final Logger log = Logger.getLogger(
      DatastorePersistingStatusReporter.class.getName());

  // Persists state to here.
  private final ShardState shardState;

  // Working set of Counters.
  private final Counters counters;

  // Status message to report.
  private String status;

  /**
   * Initialize the reporter with the given shard state.
   */
  public DatastorePersistingStatusReporter(ShardState shardState) {
    this.shardState = shardState;
    counters = shardState.getCounters();
    status = shardState.getStatusString();
  }

  @Override
  public Counter getCounter(Enum<?> name) {
    return counters.findCounter(name);
  }

  @Override
  public Counter getCounter(String group, String name) {
    return counters.findCounter(group, name);
  }

  // I can't seem to find anybody who quite knows what the purpose of this
  // method is.
  @Override
  public void progress() {
  }

  @Override
  public void setStatus(String status) {
    this.status = status;
  }

  /**
   * Persist the underlying state of this shard.
   */
  public void persist() {
    shardState.setCounters(counters);
    shardState.setStatusString(status);
    shardState.persist();
  }
}
