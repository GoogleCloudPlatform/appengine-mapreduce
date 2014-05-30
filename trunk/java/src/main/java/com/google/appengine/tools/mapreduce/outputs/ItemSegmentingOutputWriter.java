package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.OutputWriter;

import java.io.IOException;

/**
 * Segments data by using a separate writer each time {@link #shouldSegment} returns true. This is
 * used by the Merge output in the event that there are too many files to merge in one pass.
 *
 */
public abstract class ItemSegmentingOutputWriter<O> extends ForwardingOutputWriter<O> {

  private static final long serialVersionUID = 5180178926565317540L;
  private int fileCount = 0;
  private OutputWriter<O> writer;

  @Override
  public void beginShard() throws IOException {
    fileCount = 0;
    super.beginShard();
  }

  @Override
  public void write(O value) throws IOException {
    if (shouldSegment(value)) {
      writer.endSlice();
      writer.endShard();
      writer = createNextWriter(fileCount++);
      writer.setContext(getContext());
      writer.beginShard();
      writer.beginSlice();
    }
    // writer cannot be null because beginSlice and beginShard call getDelegate()
    writer.write(value);
  }

  protected abstract boolean shouldSegment(O value);

  protected abstract OutputWriter<O> createNextWriter(int fileNum);

  @Override
  protected OutputWriter<O> getDelegate() {
    if (writer == null) {
      writer = createNextWriter(fileCount++);
    }
    return writer;
  }

  @Override
  public abstract long estimateMemoryRequirement();

  @Override
  public boolean allowSliceRetry() {
    return false;
  }
}