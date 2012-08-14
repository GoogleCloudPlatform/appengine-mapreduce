// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.ReducerContext;

import java.io.IOException;

/**
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <O> type of output values produced by the reducer
 */
public class ReducerContextImpl<O> extends ReducerContext<O> {

  private final String mrJobId;
  private final int shardNumber;
  private final OutputWriter<O> output;
  private final Counters counters;

  public ReducerContextImpl(String mrJobId,
      int shardNumber,
      OutputWriter<O> output,
      Counters counters) {
    this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
    this.shardNumber = shardNumber;
    this.output = checkNotNull(output, "Null output");
    this.counters = checkNotNull(counters, "Null counters");
  }

  @Override public void emit(O value) {
    try {
      output.write(value);
    } catch (IOException e) {
      throw new RuntimeException(output + ".write(" + value + ") threw IOException", e);
    }
  }

  @Override public int getShardNumber() {
    return shardNumber;
  }

  @Override public Counters getCounters() {
    return counters;
  }

  @Override public String getJobId() {
    return mrJobId;
  }

}
