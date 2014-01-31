// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreInputStream;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;

/**
 * BlobstoreLineInputReader reads files from blobstore one line at a time.
 *
 */
class BlobstoreInputReader extends InputReader<byte[]> {

  private static final long serialVersionUID = -1869136825803030034L;

  @VisibleForTesting final long startOffset;
  @VisibleForTesting final long endOffset;
  private final String blobKey;
  private long offset = 0L;
  private final byte terminator;

  private transient LineInputStream in;


  BlobstoreInputReader(String blobKey, long startOffset, long endOffset, byte terminator) {
    Preconditions.checkArgument(endOffset >= startOffset);
    this.terminator = terminator;
    this.blobKey = blobKey;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  @Override
  public Double getProgress() {
    if (endOffset == startOffset) {
      return 1.0;
    } else {
      double currentOffset = offset + in.getBytesCount();
      return Math.min(1.0, currentOffset / (endOffset - startOffset));
    }
  }

  @Override
  public void beginShard() {
    offset = 0;
    in = null;
  }

  @Override
  public void beginSlice() throws IOException {
    Preconditions.checkState(in == null, "%s: Already initialized.", this);
    @SuppressWarnings("resource")
    InputStream blobInputStream =
        new BlobstoreInputStream(new BlobKey(blobKey), startOffset + offset);
    in = new LineInputStream(blobInputStream, endOffset - startOffset - offset, terminator);
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
  public byte[] next() throws NoSuchElementException {
    return in.next();
  }

  @Override
  public long estimateMemoryRequirement() {
    return MapReduceConstants.DEFAULT_IO_BUFFER_SIZE;
  }
}
