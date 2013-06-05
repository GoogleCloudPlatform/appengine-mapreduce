package com.google.appengine.tools.mapreduce.outputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * An {@link Output} that writes bytes to a set of Cloud Storage files, one per shard.
 * Produces a single file output (usually on a per-shard basis).
 * This format does not insert any separator characters, so it by default
 * cannot be read back with the CloudStorageLineInputReader.
 *
 */
public class GoogleCloudStorageFileOutput extends Output<ByteBuffer, List<GcsFilename>> {
  private static final long serialVersionUID = 5544139634754912546L;

  private final int shardCount;
  private final String mimeType;
  private final String fileNamePattern;
  private final String bucket;

  public GoogleCloudStorageFileOutput(
      String bucket, String fileNamePattern, String mimeType, int shardCount) {
    Preconditions.checkArgument(shardCount > 0, "Shard count not positive: %s", shardCount);
    this.bucket = bucket;
    this.mimeType = checkNotNull(mimeType, "Null mimeType");
    this.fileNamePattern = checkNotNull(fileNamePattern, "Null fileNamePattern");
    this.shardCount = shardCount;
  }

  @Override
  public List<? extends OutputWriter<ByteBuffer>> createWriters() throws IOException {
    ImmutableList.Builder<OutputWriter<ByteBuffer>> out = ImmutableList.builder();
    for (int i = 0; i < shardCount; i++) {
      GcsFilename file = new GcsFilename(bucket, String.format(fileNamePattern, i));
      out.add(new GoogleCloudStorageFileOutputWriter(file, mimeType));
    }
    return out.build();
  }

  /**
   * Returns a list of GcsFilename that has one element for each reduce shard.
   */
  @Override
  public List<GcsFilename> finish(List<? extends OutputWriter<ByteBuffer>> writers) {
    List<GcsFilename> out = Lists.newArrayList();
    for (OutputWriter<ByteBuffer> w : writers) {
      GoogleCloudStorageFileOutputWriter writer = (GoogleCloudStorageFileOutputWriter) w;
      out.add(writer.getFile());
    }
    return out;
  }

}
