package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.OutputWriter;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Segments the output base on the specified size limit. Creates a new writer if the sum of number
 * of bytes written by the current writer and number of bytes to write exceeds segment size limit
 */
public abstract class SizeSegmentingOutputWriter extends ItemSegmentingOutputWriter<ByteBuffer> {

  private static final long serialVersionUID = 7900756955061379581L;
  private final long segmentSizeLimit;
  private long bytesWritten;

  public SizeSegmentingOutputWriter(long segmentSizeLimit) {
    this.segmentSizeLimit = segmentSizeLimit;
  }

  @Override
  public void beginShard() throws IOException {
    bytesWritten = 0;
    super.beginShard();
  }

  @Override
  protected boolean shouldSegment(ByteBuffer value) {
    if (bytesWritten + value.remaining() > segmentSizeLimit) {
      return true;
    }
    return false;
  }

  @Override
  public void write(ByteBuffer value) throws IOException {
    long numOfBytesToWrite = value.remaining();
    super.write(value);
    bytesWritten += numOfBytesToWrite - value.remaining();
  }

  @Override
  protected final OutputWriter<ByteBuffer> createNextWriter(int fileNum) {
    OutputWriter<ByteBuffer> nextWriter = createWriter(fileNum);
    bytesWritten = 0;
    return nextWriter;
  }

  protected abstract OutputWriter<ByteBuffer> createWriter(int fileNum);
}
