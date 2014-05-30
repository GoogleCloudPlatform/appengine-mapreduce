// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.ReducerContext;

/**
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <O> type of output values produced by the reducer
 */
class ReducerContextImpl<O> extends BaseShardContext<O> implements ReducerContext<O> {

  ReducerContextImpl(IncrementalTaskContext c, OutputWriter<O> output) {
    super(c, output);
  }
}
