package com.google.appengine.tools.mapreduce.inputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.NoSuchElementException;

/**
 * CloudStorageLineInputReader reads files from Cloud Storage one line at a time.
 *
 */
class GoogleCloudStorageLineInputReader extends InputReader<byte[]> {
  private static final long serialVersionUID = -762091129798691745L;

  private static final transient GcsService GCS_SERVICE =
      GcsServiceFactory.createGcsService(MapReduceConstants.GCS_RETRY_PARAMETERS);
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;

  @VisibleForTesting final long startOffset;
  @VisibleForTesting final long endOffset;
  private GcsFilename file;
  private long offset;
  private final int bufferSize;
  private transient LineInputStream in;
  private final byte separator;

  GoogleCloudStorageLineInputReader(GcsFilename file, long startOffset, long endOffset,
      byte separator) {
    this(file, startOffset, endOffset, separator, DEFAULT_BUFFER_SIZE);
  }

  GoogleCloudStorageLineInputReader(GcsFilename file, long startOffset, long endOffset,
      byte separator, int bufferSize) {
    this.separator = separator;
    this.file = checkNotNull(file, "Null file");
    Preconditions.checkArgument(endOffset >= startOffset);
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.bufferSize = (bufferSize > 0) ? bufferSize : DEFAULT_BUFFER_SIZE;
  }

  @Override
  public Double getProgress() {
    if (endOffset == startOffset) {
      return 1.0;
    } else {
      double currentOffset = offset + (in == null ? 0 : in.getBytesCount());
      return Math.min(1.0, currentOffset / (endOffset - startOffset));
    }
  }

  @Override
  public void open() {
    offset = 0;
    in = null;
  }

  @Override
  public void beginSlice() throws IOException {
    Preconditions.checkState(in == null, "%s: Already initialized: %s", this, in);
    InputStream inputStream = Channels.newInputStream(
        GCS_SERVICE.openPrefetchingReadChannel(file, startOffset + offset, bufferSize));
    in = new LineInputStream(inputStream, endOffset - startOffset - offset, separator);
    skipRecordReadByPreviousShard();
  }

  /**
   * The previous record is responsible for reading past it's endOffset until a whole record is
   * read.
   */
  private void skipRecordReadByPreviousShard() {
    if (startOffset != 0L && offset == 0L) {
      try {
        in.next();
      } catch (NoSuchElementException e) {
        // Empty slice is ok.
      }
    }
  }

  @Override
  public void endSlice() throws IOException {
    offset += in.getBytesCount();
    in.close();
    in = null;
  }

  @Override
  public byte[] next() throws IOException, NoSuchElementException {
    return in.next();
  }

  @Override
  public long estimateMemoryRequirment() {
    return bufferSize * 2; // Double buffered
  }

}
