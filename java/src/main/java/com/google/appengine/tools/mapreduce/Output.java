// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * Defines where output should go.  Shards it into {@link OutputWriter}s and may
 * produce a result to be returned in {@link MapReduceResult} (see
 * {@link #finish}).
 *
 * <p>This class is really an interface that might be evolving. In order to avoid breaking
 * users when we change the interface, we made it an abstract class.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <O> type of values accepted by this output
 * @param <R> type returned by {@link #finish}
 */
public abstract class Output<O, R> implements Serializable {

  private static final long serialVersionUID = 496243337289553392L;

  private transient Context context;

  /**
   * Used internally to sets the context to be used for the processing that follows.
   */
  public void setContext(Context context) {
    this.context = context;
  }

  /**
   * Returns the current context, or null if none.
   */
  public Context getContext() {
    return context;
  }

  /**
   * Returns a list of writers, one for each shard, for this output.
   *
   * @param numShards The number of shards which should be equal to the number of
   *    {@link OutputWriter}s returned.
   */
  public abstract List<? extends OutputWriter<O>> createWriters(int numShards);

  /**
   * Returns a result to be made available through {@link MapReduceResult#getOutputResult}.
   *
   * <p>
   * This method allows the {@code Output} to inspect the final state of its {@link OutputWriter}s
   * to produce a final result object. For example, if the {@code OutputWriter}s produce blobs in
   * blobstore, {@code finish} could collect and return a list of blob IDs.
   *
   * <p>
   * Should return null if no such result makes sense for this kind of output.
   *
   * <p>
   * Called after all {@code OutputWriter}s have been closed (with {@link OutputWriter#close}). It
   * is possible for this method to be called more than once with the same {@code writers}
   * collection. It is also possible for the job to fail after this is invoked.
   *
   * <p>
   * The {@code writers} argument will contain the same writers that {@link #createWriters} returned
   * in the same order. Writers may be serialized and deserialized multiple times. typically,
   * {@code getWriter} will have been called in a different JVM.
   */
  public abstract R finish(Collection<? extends OutputWriter<O>> writers) throws IOException;
}
