package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.OutputWriter;

import java.io.Serializable;

/**
 * A factory to create new outputWriters as needed.
 * 
 * @param <O> type of values accepted by this output
 */
public interface SlicingWriterCreator<O> extends Serializable {
  /**
   * Creates a new writer. Will be called at the start of each slice.
   */
  OutputWriter<O> createNextWriter();

}