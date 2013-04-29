// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.blobstore.BlobInfoFactory;
import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * BlobstoreLineInput shards files in Blobstore on separator boundries.
 *
 */
public class BlobstoreInput extends Input<byte[]> {
// --------------------------- STATIC FIELDS ---------------------------

  private static final long MIN_SHARD_SIZE = 1024L;

  private static final long serialVersionUID = 2235444204028285444L;

// ------------------------------ FIELDS ------------------------------

  private final String blobKey;
  private final byte separator;
  private final int shardCount;

// --------------------------- CONSTRUCTORS ---------------------------

  public BlobstoreInput(String blobKey, byte separator, int shardCount) {
    this.blobKey = blobKey;
    this.separator = separator;
    this.shardCount = shardCount;
  }

// ------------------------ IMPLEMENTING METHODS ------------------------

  @Override
  public List<? extends InputReader<byte[]>> createReaders() {
    long blobSize = new BlobInfoFactory().loadBlobInfo(new BlobKey(blobKey)).getSize();
    return split(blobKey, blobSize, shardCount);
  }

// -------------------------- INSTANCE METHODS --------------------------

  private List<? extends InputReader<byte[]>> split(String blobKey,
      long blobSize, int shardCount) {
    Preconditions.checkNotNull(blobKey);
    Preconditions.checkArgument(shardCount > 0);
    Preconditions.checkArgument(blobSize >= 0);

    // Sanity check
    if (shardCount * MIN_SHARD_SIZE > blobSize) {
      shardCount = (int) (blobSize / MIN_SHARD_SIZE) + 1;
    }

    long splitLength = blobSize / shardCount;

    List<BlobstoreInputReader> result = new ArrayList<BlobstoreInputReader>();

    long startOffset = 0L;
    for (int i = 1; i < shardCount; i++) {
      long endOffset = i * splitLength;
      result.add(new BlobstoreInputReader(blobKey, startOffset, endOffset, separator));
      startOffset = endOffset;
    }
    result.add(new BlobstoreInputReader(blobKey, startOffset, blobSize, separator));
    return result;
  }
}
