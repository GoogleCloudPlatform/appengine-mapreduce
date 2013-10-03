// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import java.io.IOException;
import java.io.Serializable;
import java.util.NoSuchElementException;

/**
 * Reads data from an input source.
 *
 * <p>Created by {@link Input} to read input for a given shard.  Reads input for
 * a shard as a number of slices, where the slicing is up to the caller.
 *
 * <p>Each slice is read by calling {@link #beginSlice}, then {@link #next}
 * and/or {@link #getProgress} any number of times in any order, then
 * {@link #endSlice}.  Between two slices, before the first slice, or after the
 * final slice, the {@code InputReader} may go through serialization and
 * deserialization.
 *
 * <p>If a slice is aborted, there is no guarantee whether {@link #endSlice}
 * will be called; however, if it is not called, the {@code InputReader} will
 * not be serialized.  If the slice is retried later, the {@code InputReader}
 * serialized after the previous slice will be deserialized again.
 *
 * <p>This class is really an interface that might be evolving. In order to avoid breaking
 * users when we change the interface, we made it an abstract class.
 *
 *
 * @param <I> type of values produced by this input
 */
public abstract class InputReader<I> implements Serializable {
  private static final long serialVersionUID = 680562312715017093L;

  /**
   * Returns the next input value, or throws {@link NoSuchElementException}
   * when there is no more input data. This is done rather than providing a
   * hasNext() function, to allow the implementation to deal with streams and
   * remain serializable, even if the item being read is not.
   */
  public abstract I next() throws IOException, NoSuchElementException;

  /**
   * Returns the relative progress reading this input as a number from 0 to 1.
   * Returns null if relative progress cannot be determined.
   */
  public Double getProgress() {
    return null;
  }

  /**
   * Prepares the {@code InputReader} for reading, after possibly having gone
   * through serialization and deserialization.
   */
  public void beginSlice() throws IOException {}

  /**
   * Prepares the {@code InputReader} for serialization.
   */
  public void endSlice() throws IOException {}

  /**
   * Performs setup at the beginning of the shard. This method is invoked before the first call to
   * {@link #beginSlice}. It will not be invoked again unless the shard restarts. When a shard is
   * restarted, this method is invoked and the input should be read from the beginning.
   */
  public void open() throws IOException {}

  /**
   * Called after endSlice if there will not be any subsequent calls to beginSlice or next.
   * This may be used to teardown or finalize any state if required.
   */
  public void close() throws IOException {}

}
