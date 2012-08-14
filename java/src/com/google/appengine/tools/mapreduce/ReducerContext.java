// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

/**
 * Context for {@link Reducer} execution.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <O> type of output values produced by the reducer
 */
public abstract class ReducerContext<O> extends WorkerContext {

  /**
   * Emits a value to the MapReduce output.
   */
  public abstract void emit(O value);

}
