package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.OutputWriter;

import java.io.IOException;

/**
 * Segments data by using a separate writer for each slice. This is used by the Sort output so that
 * there can be multiple sorted files per reducer.
 *
 * @param <O> type of values accepted by this output
 * @param <WriterT> type of the output writer being written to
 *
 */
public abstract class SlicingOutputWriter<O, WriterT extends OutputWriter<O>> extends
    OutputWriter<O> {

  private static final long serialVersionUID = -2846649020412508288L;
  private int sliceCount;
  private transient OutputWriter<O> writer;

  @Override
  public void beginShard() {
    sliceCount = 0;
  }

  /**
   * Creates a new writer.
   */
  @Override
  public void beginSlice() throws IOException {
    writer = createWriter(sliceCount++);
    writer.setContext(getContext());
    writer.beginShard();
    writer.beginSlice();
  }

  /**
   * Creates a new writer. This is called once per slice
   */
  protected abstract WriterT createWriter(int sliceNumber);

  /**
   * closes the current writer.
   */
  @Override
  public void endSlice() throws IOException {
    writer.endSlice();
    writer.endShard();
    writer = null;
  }

  @Override
  public void write(O value) throws IOException {
    writer.write(value);
  }

  @Override
  public boolean allowSliceRetry() {
    return true;
  }

  @Override
  public abstract long estimateMemoryRequirement();
}
