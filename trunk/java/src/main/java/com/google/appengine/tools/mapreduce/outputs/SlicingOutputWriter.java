package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.OutputWriter;

import java.io.IOException;

/**
 * Segments data by using a separate writer for each slice. This is used by the Sort output so that
 * there can be multiple sorted files per reducer.
 *
 * @param <O> type of values accepted by this output
 * @param <CreatorT> type of the SlicingWriterCreator provided.
 *
 */
public class SlicingOutputWriter<O, CreatorT extends SlicingWriterCreator<O>> extends
    OutputWriter<O> {

  private static final long serialVersionUID = 7374361726571559544L;
  private CreatorT creator;
  private transient OutputWriter<O> writer;

  public SlicingOutputWriter(CreatorT creator) {
    this.creator = creator;
  }

  /**
   * Creates a new writer.
   */
  @Override
  public void beginSlice() throws IOException {
    writer = getCreator().createNextWriter();
    writer.beginSlice();
  }

  /**
   * closes the current writer.
   */
  @Override
  public void endSlice() throws IOException {
    writer.endSlice();
    writer.close();
    writer = null;
  }

  @Override
  public void write(O value) throws IOException {
    writer.write(value);
  }

  @Override
  public void close() throws IOException {
    // Done in endSlice();
  }

  public CreatorT getCreator() {
    return creator;
  }

}
