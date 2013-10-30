// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import java.io.Serializable;

/**
 * Result of a {@link MapReduceJob}.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <R> type of result produced by the {@link Output}
 */
public interface MapReduceResult<R> extends Serializable {

  /**
   * Returns the result from {@link Output#finish} or {@code null} if completed unsuccessfully.
   */
  R getOutputResult();

  /**
   * Returns the counter values at the end of the MapReduce.
   */
  Counters getCounters();
}
