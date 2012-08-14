// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import java.io.IOException;
import java.io.Serializable;
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

  /**
   * Returns a list of writers for this output.  It is the {@code Output}'s
   * responsibility to determine an appropriate number of writers to split into.
   * This could be specified by the user or algorithmically determined.
   *
   * <p>The number of output writers returned determines the number of reduce shards.
   */
  public abstract List<? extends OutputWriter<O>> createWriters() throws IOException;

  /**
   * Returns a result to be made available through
   * {@link MapReduceResult#getOutputResult}.
   *
   * <p>This method allows the {@code Output} to inspect the final state of its
   * {@link OutputWriter}s to produce a final result object.  For example, if
   * the {@code OutputWriter}s produce blobs in blobstore, {@code finish} could
   * collect and return a list of blob IDs.
   *
   * <p>Should return null if no such result makes sense for this kind of output.
   *
   * <p>Called after all {@code OutputWriter}s have been closed (with
   * {@link OutputWriter#close}).
   *
   * <p>The {@code writers} argument will contain the same writers that
   * {@link #createWriters} returned (modulo serialization -- typically,
   * {@code getWriter} will have been called in a different JVM).
   */
  public abstract R finish(List<? extends OutputWriter<O>> writers) throws IOException;

}
