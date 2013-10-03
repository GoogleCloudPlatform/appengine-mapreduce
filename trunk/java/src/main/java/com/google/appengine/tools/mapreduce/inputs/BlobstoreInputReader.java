// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreInputStream;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * BlobstoreLineInputReader reads files from blobstore one line at a time.
 *
 */
class BlobstoreInputReader extends InputReader<byte[]> {
  // --------------------------- STATIC FIELDS ---------------------------

  private static final long serialVersionUID = -1869136825803030034L;

  // ------------------------------ FIELDS ------------------------------

  @VisibleForTesting final long startOffset;
  @VisibleForTesting final long endOffset;
  private final String blobKey;
  private long offset = 0L;

  private transient LineInputStream in;

  private final byte terminator;

  // --------------------------- CONSTRUCTORS ---------------------------

  BlobstoreInputReader(String blobKey, long startOffset, long endOffset, byte terminator) {
    this.terminator = terminator;
    this.blobKey = blobKey;
    Preconditions.checkArgument(endOffset >= startOffset);
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  // --------------------------- METHODS ---------------------------

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
  public void open() {
    offset = 0;
    in = null;
  }

  @Override
  public void beginSlice() throws IOException {
    Preconditions.checkState(in == null, "%s: Already initialized.", this);
    in = new LineInputStream(
        new BlobstoreInputStream(new BlobKey(blobKey), startOffset + offset),
        endOffset - startOffset - offset, terminator);
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

}
