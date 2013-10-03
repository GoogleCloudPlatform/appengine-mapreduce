package com.google.appengine.tools.mapreduce.inputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.InputReader;

import java.io.IOException;
import java.io.InputStream;

/**
 * Base class to build an InputReader from an InputStream
 *
 *
 * @param <I> type of values read by this input
 */
public abstract class StreamInputReader<I> extends InputReader<I> {

  private static final long serialVersionUID = -8391447352645460518L;
  private long bytesRead;
  protected final long length;
  private final InputStream in;

  StreamInputReader(InputStream in, long length) {
    this.in = checkNotNull(in);
    this.length = length;
  }

  @Override
  public Double getProgress() {
    if (length == 0L) {
      return null;
    }
    return bytesRead / (double) length;
  }

  public int read(byte[] b, int off, int len) throws IOException {
    int result = in.read(b, off, len);
    if (result != -1) {
      bytesRead += result;
    }
    return result;
  }

  protected long getBytesRead() {
    return bytesRead;
  }

  /**
   * Calls read on the underlying stream.
   */
  protected int read() throws IOException {
    int result = in.read();
    if (result != -1) {
      bytesRead++;
    }
    return result;
  }

  /**
   * Calls close on the underlying stream.
   */
  protected void close() throws IOException {
    in.close();
  }

}
