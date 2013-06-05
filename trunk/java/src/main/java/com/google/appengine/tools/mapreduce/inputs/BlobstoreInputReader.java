// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreInputStream;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.inputs.InputStreamIterator.OffsetRecordPair;
import com.google.common.base.Preconditions;
import com.google.common.io.CountingInputStream;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * BlobstoreLineInputReader reads files from blobstore one line at a time.
 */
class BlobstoreInputReader extends InputReader<byte[]> {
// --------------------------- STATIC FIELDS ---------------------------

  private static final long serialVersionUID = -1869136825803030034L;

// ------------------------------ FIELDS ------------------------------

  /*VisibleForTesting*/ long startOffset;
  /*VisibleForTesting*/ long endOffset;
  private String blobKey;
  private byte terminator;
  private long offset = 0L;
  private transient CountingInputStream input;
  private transient Iterator<OffsetRecordPair> recordIterator;

// --------------------------- CONSTRUCTORS ---------------------------

  BlobstoreInputReader(String blobKey, long startOffset, long endOffset, byte terminator) {
    this.blobKey = blobKey;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.terminator = terminator;
  }

// --------------------------- METHODS ---------------------------

  private void checkInitialized() {
    Preconditions.checkState(recordIterator != null, "%s: Not initialized", this);
  }

  @Override
  public byte[] next() {
    checkInitialized();
    if (!recordIterator.hasNext()) {
      throw new NoSuchElementException();
    }

    // TODO(ohler): simplify by removing OffsetRecordPair
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
  public void beginSlice() throws IOException {
    Preconditions.checkState(recordIterator == null, "%s: Already initialized: %s",
        this, recordIterator);
    input = new CountingInputStream(
            new BlobstoreInputStream(new BlobKey(blobKey), startOffset + offset));
    recordIterator = new InputStreamIterator(input, endOffset - startOffset - offset,
        startOffset != 0L && offset == 0L,
        terminator);
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
