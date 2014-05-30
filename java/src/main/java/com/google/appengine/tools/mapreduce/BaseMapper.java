// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

/**
 * Abstract class for Map function.
 *
 * @param <I> type of input received
 * @param <O> type of intermediate values produced
 */
abstract class BaseMapper<I, O, C extends WorkerContext<O>> extends Worker<C> {

  private static final long serialVersionUID = -6551234158528563026L;

  /**
   * Processes a single input value, emitting output through the context returned by
   * {@link Worker#getContext} or {@link #emit}.
   */
  public abstract void map(I value);

  /**
   * Syntactic sugar for {@code getContext().emit(value)}
   */
  protected void emit(O value) {
    getContext().emit(value);
  }
}
