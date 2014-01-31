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
 * <p>
 * {@link #beginShard} is called before any calls to {@link #beginSlice} or {@link #next}
 * to setup the reader.
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
 * <p>
 * At the end of the final slice, {@link #endShard()} will be called after {@link #endSlice()}.
 *
 * @author ohler@google.com (Christian Ohler)
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
   *
   * @throws IOException in the event of failure
   */
  public void beginSlice() throws IOException {}

  /**
   * Prepares the {@code InputReader} for serialization.
   *
   * @throws IOException in the event of failure
   */
  public void endSlice() throws IOException {}

  /**
   * @deprecated Override beginShard instead.
   * @throws IOException in the event of failure
   */
  @Deprecated
  public void open() throws IOException {}

  /**
   * Performs setup at the beginning of the shard. This method is invoked before the first call to
   * {@link #beginSlice}. It will not be invoked again unless the shard restarts. When a shard is
   * restarted, this method is invoked and the input should be read from the beginning.
   *
   * @throws IOException in the event of failure
   */
  public void beginShard() throws IOException {
    open();
  }

  /**
   * @deprecated Override endShard instead.
   * @throws IOException in the event of failure
   */
  @Deprecated
  public void close() throws IOException {}

  /**
   * Called after endSlice if there will not be any subsequent calls to beginSlice or next.
   * This may be used to teardown or finalize any state if required.
   *
   * @throws IOException in the event of failure
   */
  public void endShard() throws IOException {
    close();
  }

  /**
   * Returns the estimated memory that will be used by this reader in bytes.
   * (This is normally just set to the size of the buffers used by the implementation)
   */
  public long estimateMemoryRequirement() {
    return 0;
  }
}
