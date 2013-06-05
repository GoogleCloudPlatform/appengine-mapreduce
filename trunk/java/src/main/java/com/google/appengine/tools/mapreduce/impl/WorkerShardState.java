// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import java.io.Serializable;

/**
 * @author ohler@google.com (Christian Ohler)
 */
public class WorkerShardState implements Serializable {

  private static final long serialVersionUID = 143150880872942204L;

  private final long workerCallCount;
  private final long mostRecentUpdateTimeMillis;
  /*Nullable*/ private final String lastWorkItem;

  public WorkerShardState(long workerCallCount,
      long mostRecentUpdateTimeMillis,
      /*Nullable*/ String lastWorkItem) {
    this.workerCallCount = workerCallCount;
    this.mostRecentUpdateTimeMillis = mostRecentUpdateTimeMillis;
    this.lastWorkItem = lastWorkItem;
  }

  public long getWorkerCallCount() {
    return workerCallCount;
  }

  public long getMostRecentUpdateTimeMillis() {
    return mostRecentUpdateTimeMillis;
  }

  /*Nullable*/ public String getLastWorkItem() {
    return lastWorkItem;
  }

  @Override public String toString() {
    return getClass().getSimpleName() + "("
        + workerCallCount + ", "
        + mostRecentUpdateTimeMillis + ", "
        + lastWorkItem
        + ")";
  }

}
