// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import java.io.IOException;
import java.io.Serializable;

/**
 * Writes key-value pairs.
 *
 * <p>
 * Created by {@link Output} to write input for a given shard. Writers output for a shard as a
 * number of slices, where the slicing is up to the caller.
 *
 * <p>
 * {@link #open} is called before any calls to {@link #beginSlice} or {@link #write} to setup the
 * writer.
 *
 * <p>
 * Each slice is written by calling {@link #beginSlice}, then {@link #write} any number of times,
 * then {@link #endSlice}. Before the first slice, in between two slices, or after the final slice,
 * the {@code OutputWriter} may go through serialization and deserialization.
 *
 * <p>
 * At the end of the final slice, {@link #close()} will be called after {@link #endSlice()}.
 *
 * <p>
 * If a slice is aborted, there is no guarantee whether {@link #endSlice} will be called; however,
 * if it is not called, the {@code OutputWriter} will not be serialized. If the slice is retried
 * later, the {@code OutputWriter} serialized after the previous slice will be deserialized again.
 *
 * <p>
 * This class is really an interface that might be evolving. In order to avoid breaking users when
 * we change the interface, we made it an abstract class.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <O> type of values accepted by this output
 */
public abstract class OutputWriter<O> implements Serializable {
  private static final long serialVersionUID = 5225114373913821210L;

  /**
   * Prepares the writer for writing after possibly having gone through
   * serialization or deserialization.
   */
  public void beginSlice() throws IOException {}

  /**
   * Writes a value to the output.
   */
  public abstract void write(O value) throws IOException;

  /**
   * Prepares the writer for possible serialization.
   */
  public void endSlice() throws IOException {}
  
  /**
   * Will be called once before any calls to write. Prepares the writer for processing, after
   * possibly having gone through serialization and deserialization.
   */
  public void open() throws IOException {}

  /**
   * Returns the estimated mininum memory that will be used by this writer. 
   * (This is normally just set to the size of the buffers used by the implementation)
   */
  public long estimateMemoryRequirment() {
    return 0;
  }
  
  /**
   * Called when no more output will be written to this writer.
   */
  public abstract void close() throws IOException;

}
