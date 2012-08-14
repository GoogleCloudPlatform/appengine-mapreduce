// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.outputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.logging.Logger;

/**
 * An {@link Output} that writes bytes to a set of blob files, one per shard.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class BlobFileOutput extends Output<ByteBuffer, List<AppEngineFile>> {
  private static final long serialVersionUID = 868276534742230776L;

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(BlobFileOutput.class.getName());

  private final int shardCount;
  private final String mimeType;
  private final String fileNamePattern;

  public BlobFileOutput(String fileNamePattern,
      String mimeType,
      int shardCount) {
    Preconditions.checkArgument(shardCount > 0, "Shard count not positive: %s", shardCount);
    this.mimeType = checkNotNull(mimeType, "Null mimeType");
    this.fileNamePattern = checkNotNull(fileNamePattern, "Null fileNamePattern");
    this.shardCount = shardCount;
  }

  @Override public List<? extends OutputWriter<ByteBuffer>> createWriters() {
    ImmutableList.Builder<OutputWriter<ByteBuffer>> out = ImmutableList.builder();
    for (int i = 0; i < shardCount; i++) {
      out.add(new BlobFileOutputWriter(String.format(fileNamePattern, i), mimeType));
    }
    return out.build();
  }

  /**
   * Returns a list of AppEngineFiles that has one element for each reduce
   * shard.  Each element is either an {@link AppEngineFile} or null (if that
   * reduce shard emitted no data).
   */
  @Override public List<AppEngineFile> finish(List<? extends OutputWriter<ByteBuffer>> writers) {
    List<AppEngineFile> out = Lists.newArrayList();
    for (OutputWriter<ByteBuffer> w : writers) {
      @SuppressWarnings("unchecked")
      BlobFileOutputWriter writer = (BlobFileOutputWriter) w;
      out.add(writer.getFile());
    }
    return out;
  }

}
