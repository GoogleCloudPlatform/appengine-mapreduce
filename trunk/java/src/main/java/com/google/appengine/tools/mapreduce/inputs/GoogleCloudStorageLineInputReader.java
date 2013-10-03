package com.google.appengine.tools.mapreduce.inputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

/**
 * CloudStorageLineInputReader reads files from Cloud Storage one line at a time.
 */
class GoogleCloudStorageLineInputReader extends InputReader<byte[]> {
  private static final long serialVersionUID = -762091129798691745L;

  private static final transient GcsService GCS_SERVICE =
      GcsServiceFactory.createGcsService(MapReduceConstants.GCS_RETRY_PARAMETERS);
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;

  /*VisibleForTesting*/ long startOffset;
  /*VisibleForTesting*/ long endOffset;
  private GcsFilename file;
  private byte separator;
  private long offset = 0L;
  private int bufferSize;
  private transient LineInputReader lineReader;

  GoogleCloudStorageLineInputReader(
      GcsFilename file, long startOffset, long endOffset, byte separator) {
    this(file, startOffset, endOffset, separator, DEFAULT_BUFFER_SIZE);
  }

  GoogleCloudStorageLineInputReader(
      GcsFilename file, long startOffset, long endOffset, byte separator, int bufferSize) {
    this.file = checkNotNull(file, "Null file");
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.separator = separator;
    this.bufferSize = (bufferSize > 0) ? bufferSize : DEFAULT_BUFFER_SIZE;
  }

  private void checkInitialized() {
    Preconditions.checkState(lineReader != null, "%s: Not initialized", this);
  }

  @Override
  public byte[] next() {
    checkInitialized();
    return lineReader.next();
  }

  @Override
  public Double getProgress() {
    checkInitialized();
    if (endOffset == startOffset) {
      return 1.0;
    } else {
      double currentOffset = offset + lineReader.getBytesRead();
      return Math.min(1.0, currentOffset / (endOffset - startOffset));
    }
  }

  @Override
  public void beginSlice() {
    Preconditions.checkState(
        lineReader == null, "%s: Already initialized: %s", this, lineReader);
    InputStream input = Channels.newInputStream(
        GCS_SERVICE.openPrefetchingReadChannel(file, startOffset + offset, bufferSize));
    lineReader = new LineInputReader(
        input, endOffset - startOffset - offset, startOffset != 0L && offset == 0L, separator);
  }

  @Override
  public void endSlice() throws IOException {
    checkInitialized();
    offset += lineReader.getBytesRead();
    lineReader.close();
    // Un-initialize to make checkInitialized() effective.
    lineReader = null;
  }

}
