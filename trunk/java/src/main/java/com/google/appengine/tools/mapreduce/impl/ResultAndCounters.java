// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.MapReduceResult;

/**
 * Holds a result and a CountersImpl.  Implements {@link MapReduceResult} but is
 * also used as an intermediate result of the map phase.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <R> type of result
 */
public class ResultAndCounters<R> implements MapReduceResult<R> {
  private static final long serialVersionUID = 237070477689138395L;

  /*Nullable*/ private final R outputResult;
  private final CountersImpl counters;

  public ResultAndCounters(/*Nullable*/ R outputResult,
      CountersImpl counters) {
    this.outputResult = outputResult;
    this.counters = checkNotNull(counters, "Null counters");
  }

  @Override /*Nullable*/ public R getOutputResult() {
    return outputResult;
  }

  @Override public CountersImpl getCounters() {
    return counters;
  }

  @Override public String toString() {
    return getClass().getSimpleName() + "("
        + outputResult + ", "
        + counters
        + ")";
  }

}
