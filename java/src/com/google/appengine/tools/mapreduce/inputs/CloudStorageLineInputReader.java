package com.google.appengine.tools.mapreduce.inputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.inputs.InputStreamIterator.OffsetRecordPair;
import com.google.common.base.Preconditions;
import com.google.common.io.CountingInputStream;

import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * CloudStorageLineInputReader reads files from Cloud Storage one line at a time.
 */
class CloudStorageLineInputReader extends InputReader<byte[]> {
  private static final long serialVersionUID = -762091129798691745L;

  private static final transient GcsService GCS_SERVICE = GcsServiceFactory.createGcsService();
  private static final int BUFFER_SIZE = 1024 * 1024;

  /*VisibleForTesting*/ long startOffset;
  /*VisibleForTesting*/ long endOffset;
  private GcsFilename file;
  private byte separator;
  private long offset = 0L;
  private transient CountingInputStream input;
  private transient Iterator<OffsetRecordPair> recordIterator;

  CloudStorageLineInputReader(GcsFilename file, long startOffset, long endOffset, byte separator) {
    this.file = checkNotNull(file, "Null file");
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.separator = separator;
  }

  private void checkInitialized() {
    Preconditions.checkState(recordIterator != null, "%s: Not initialized", this);
  }

  @Override
  public byte[] next() {
    checkInitialized();
    if (!recordIterator.hasNext()) {
      throw new NoSuchElementException();
    }
    // TODO(user): simplify by removing OffsetRecordPair
    return recordIterator.next().getRecord();
  }

  @Override
  public Double getProgress() {
    checkInitialized();
    if (endOffset == startOffset) {
      return 1.0;
    } else {
      double currentOffset = offset + input.getCount();
      return Math.min(1.0, currentOffset / (endOffset - startOffset));
    }
  }

  @Override
  public void beginSlice() {
    Preconditions.checkState(
        recordIterator == null, "%s: Already initialized: %s", this, recordIterator);
    input = new CountingInputStream(Channels.newInputStream(
        GCS_SERVICE.openPrefetchingReadChannel(file, startOffset + offset, BUFFER_SIZE)));
    recordIterator = new InputStreamIterator(
        input, endOffset - startOffset - offset, startOffset != 0L && offset == 0L, separator);
  }

  @Override
  public void endSlice() throws IOException {
    checkInitialized();
    offset += input.getCount();
    input.close();
    // Un-initialize to make checkInitialized() effective.
    input = null;
    recordIterator = null;
  }

}
