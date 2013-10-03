package com.google.appengine.tools.mapreduce.outputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

/**
 * An {@link Output} that writes bytes to a set of Cloud Storage files, one per shard.
 * Produces a single file output (usually on a per-shard basis).
 * This format does not insert any separator characters, so it by default
 * cannot be read back with the CloudStorageLineInputReader.
 *
 */
public class GoogleCloudStorageFileOutput extends Output<ByteBuffer, GoogleCloudStorageFileSet> {
  private static final long serialVersionUID = 5544139634754912546L;

  private final int shardCount;
  private final String mimeType;
  private final String fileNamePattern;
  private final String bucket;

  /**
   * Creates {@code shardCount} number of output files who's names follow the provided pattern in
   * the specified bucket.
   *
   * @param fileNamePattern a Java format string {@link java.util.Formatter} containing one int
   *        argument for the shard number.
   * @param mimeType The string to be passed as the mimeType to GCS.
   */
  public GoogleCloudStorageFileOutput(
      String bucket, String fileNamePattern, String mimeType, int shardCount) {
    Preconditions.checkArgument(shardCount > 0, "Shard count not positive: %s", shardCount);
    this.bucket = checkNotNull(bucket);
    this.mimeType = checkNotNull(mimeType, "Null mimeType");
    this.fileNamePattern = checkNotNull(fileNamePattern, "Null fileNamePattern");
    this.shardCount = shardCount;
  }

  @Override
  public List<GoogleCloudStorageFileOutputWriter> createWriters() {
    ImmutableList.Builder<GoogleCloudStorageFileOutputWriter> out = ImmutableList.builder();
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
  public GoogleCloudStorageFileSet finish(Collection<? extends OutputWriter<ByteBuffer>> writers) {
    List<String> out = Lists.newArrayList();
    for (OutputWriter<ByteBuffer> w : writers) {
      GoogleCloudStorageFileOutputWriter writer = (GoogleCloudStorageFileOutputWriter) w;
      out.add(writer.getFile().getObjectName());
    }
    return new GoogleCloudStorageFileSet(bucket, out);
  }

  @Override
  public int getNumShards() {
    return shardCount;
  }

}
