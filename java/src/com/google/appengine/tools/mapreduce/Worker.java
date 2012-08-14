// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import java.io.Serializable;

/**
 * Base class for {@link Mapper} and {@link Reducer}.
 *
 * <p>Created during a {@link MapReduceJob} to process a given shard.  Processes
 * data for zero or more shards, each subdivided into zero or more slices, where
 * sharding and slicing is up to the caller.
 *
 * <p>Each shard starts with a call to {@link #beginShard}, then processes zero
 * or more slices, then finishes with a call to {@link #endShard}.  Between two
 * shards, before the first shard, or after the final shard, the worker may go
 * through serialization and deserialization.
 *
 * <p>If a shard is aborted, there is no guarantee whether {@link #endShard}
 * will be called; however, if it is not called, the worker will not be
 * serialized or used further in any way.  If the shard is retried later, the
 * worker serialized after the previous shard will be deserialized again.
 *
 * <p>A worker may be used for processing multiple shards in parallel; in that
 * case, it is deserialized multiple times to produce multiple copies.  Each
 * copy will process only one shard at a time.
 *
 * <p>Each slice starts with a call to {@link #beginSlice}, then performs the
 * actual work with zero or more calls to {@link Mapper#map} or
 * {@link Reducer#reduce}, then {@link #endSlice}.  Between two slices, before
 * the first slice, or after the final slice, the worker may go through
 * serialization and deserialization.
 *
 * <p>If a slice is aborted, there is no guarantee whether {@link #endSlice}
 * will be called; however, if it is not called, the worker will not be
 * serialized or used further in any way.  If the slice is retried later, the
 * {@code Worker} serialized after the previous slice will be deserialized
 * again.
 *
 * <p>Slices of the same shard are processed by the same worker sequentially.
 *
 * <p>This class is really an interface that might be evolving.  In order to
 * avoid breaking users when we change the interface, we made it an abstract
 * class.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <C> type of context required by this worker
 */
public abstract class Worker<C extends WorkerContext> implements Serializable {
  private static final long serialVersionUID = 261775900734965853L;

  private final LifecycleListenerRegistry lifecycleListenerRegistry =
      LifecycleListenerRegistry.create();

  private transient C context;

  /**
   * Sets the context to be used for the processing that follows.  Called after
   * deserialization before {@link #beginShard} or {@link #beginSlice}.
   */
  public void setContext(C context) {
    this.context = context;
  }

  /**
   * Returns the current context, or null if none.
   */
  protected C getContext() {
    return context;
  }

  /**
   * Returns this worker's lifecycle listener registry.
   */
  public LifecycleListenerRegistry getLifecycleListenerRegistry() {
    return lifecycleListenerRegistry;
  }

  /**
   * Prepares the worker for processing a new shard, after possibly having gone
   * through serialization and deserialization, and prepares it for possible
   * serialization.
   */
  public void beginShard() {}

  /**
   * Notifies the worker that there is no more data to process in the current
   * shard, and prepares it for possible serialization.
   */
  public void endShard() {}

  /**
   * Prepares the worker for processing a new slice, after possibly having gone
   * through serialization and deserialization.
   *
   * <p>This does not send the event to the lifecycle listeners.
   */
  public void beginSlice() {}

  /**
   * Notifies the worker that there is no more data to process in the current
   * slice, and prepares it for possible serialization.
   *
   * <p>This does not send the event to the lifecycle listeners.
   */
  public void endSlice() {}

}
