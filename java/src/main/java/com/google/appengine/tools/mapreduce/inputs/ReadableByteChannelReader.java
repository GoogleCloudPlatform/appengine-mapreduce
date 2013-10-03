package com.google.appengine.tools.mapreduce.inputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.InputReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * Base class for a InputReader which receives data form a ReadableByteChannel.
 *
 */
abstract class ReadableByteChannelReader extends InputReader<ByteBuffer> {

  private static final long serialVersionUID = 8136460896636789786L;
  private long bytesRead;
  private final ReadableByteChannel in;

  ReadableByteChannelReader(ReadableByteChannel in) {
    this.in = checkNotNull(in);
  }

  @Override
  public Double getProgress() {
    return null;
  }

  protected int read(ByteBuffer result) throws IOException {
    int totalRead = 0;
    while (result.hasRemaining()) {
      int read = in.read(result);
      if (read == -1) {
        if (totalRead == 0) {
          totalRead = -1;
        }
        break;
      } else {
        totalRead += read;
        bytesRead += read;
      }
    }
    return totalRead;
  }

  protected long getBytesRead() {
    return bytesRead;
  }

  /**
   * Calls close on the underlying stream.
   */
  protected void close() throws IOException {
    in.close();
  }

}
