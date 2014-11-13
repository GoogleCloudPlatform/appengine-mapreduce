package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.Counters;

import java.io.Serializable;

/**
 * Context used by incremental tasks.
 */
public class IncrementalTaskContext implements Serializable {

  private static final long serialVersionUID = -3696404958641945634L;
  private final String workerCallsCounterName;
  private final String workerMillisCounterName;
  private final String jobId;
  private final int shardNumber;
  private final int shardCount;
  private final Counters counters;
  private String lastWorkItem;

  public IncrementalTaskContext(String jobId, int shardNumber, int shardCount,
      String workerCallsCounterName, String workerMillisCounterName) {
    this.jobId = checkNotNull(jobId, "Null jobId");
    this.shardNumber = shardNumber;
    this.shardCount = shardCount;
    this.workerCallsCounterName =
        checkNotNull(workerCallsCounterName, "Null workerCallsCounterName");
    this.workerMillisCounterName =
        checkNotNull(workerMillisCounterName, "Null workerMillisCounterName");
    this.counters = new CountersImpl();
  }

  public Counters getCounters() {
    return counters;
  }

  public String getJobId() {
    return jobId;
  }

  public int getShardNumber() {
    return shardNumber;
  }

  public int getShardCount() {
    return shardCount;
  }

  public long getWorkerCallCount() {
    return getCounters().getCounter(workerCallsCounterName).getValue();
  }

  long getWorkerTimeMillis() {
    return getCounters().getCounter(workerMillisCounterName).getValue();
  }

  public String getLastWorkItemString() {
    return lastWorkItem;
  }

  void setLastWorkItemString(String lastWorkItem) {
    this.lastWorkItem = lastWorkItem;
  }

  void incrementWorkerCalls(long workerCalls) {
    getCounters().getCounter(workerCallsCounterName).increment(workerCalls);
  }

  void incrementWorkerMillis(long millis) {
    getCounters().getCounter(workerMillisCounterName).increment(millis);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[jobId=" + jobId + ", shardNumber=" + shardNumber
        + ", shardCount=" + shardCount + ", lastWorkItem=" + lastWorkItem + ", workerCallCount="
        + getWorkerCallCount() + ", workerTimeMillis=" + getWorkerTimeMillis() + "]";
  }
}
