// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.MapReduceResult;

/**
 * Implementation of {@link MapReduceResult}.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <R> type of result
 */
public class MapReduceResultImpl<R> implements MapReduceResult<R> {
  private static final long serialVersionUID = 237070477689138395L;

  /*Nullable*/ private final R outputResult;
  private final Counters counters;

  public MapReduceResultImpl(/*Nullable*/ R outputResult,
      Counters counters) {
    this.outputResult = outputResult;
    this.counters = checkNotNull(counters, "Null counters");
  }

  @Override /*Nullable*/ public R getOutputResult() {
    return outputResult;
  }

  @Override public Counters getCounters() {
    return counters;
  }

  @Override public String toString() {
    return getClass().getSimpleName() + "("
        + outputResult + ", "
        + counters
        + ")";
  }

}
