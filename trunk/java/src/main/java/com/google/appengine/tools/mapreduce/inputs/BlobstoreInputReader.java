// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreInputStream;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;

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
  private transient LineInputReader lineReader;

// --------------------------- CONSTRUCTORS ---------------------------

  BlobstoreInputReader(String blobKey, long startOffset, long endOffset, byte terminator) {
    this.blobKey = blobKey;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.terminator = terminator;
  }

// --------------------------- METHODS ---------------------------

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
  public void beginSlice() throws IOException {
    Preconditions.checkState(lineReader == null, "%s: Already initialized: %s",
        this, lineReader);
    InputStream input =
            new BlobstoreInputStream(new BlobKey(blobKey), startOffset + offset);
    lineReader = new LineInputReader(input, endOffset - startOffset - offset,
        startOffset != 0L && offset == 0L,
        terminator);
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
