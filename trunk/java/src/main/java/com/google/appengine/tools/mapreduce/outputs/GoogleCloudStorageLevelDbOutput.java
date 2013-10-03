package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * An composition of {@link GoogleCloudStorageFileOutput} with
 * {@link GoogleCloudStorageLevelDbOutput} that pads blocks to GCS write boundaries on end of slice.
 *
 */
public class GoogleCloudStorageLevelDbOutput extends Output<ByteBuffer, GoogleCloudStorageFileSet> {
  private static final long serialVersionUID = -8743458990620039721L;

  private final GoogleCloudStorageFileOutput output;

  /**
   * Creates {@code shardCount} number of LevelDb output files who's names follow the provided
   * pattern in the specified bucket.
   *
   * @param fileNamePattern a Java format string {@link java.util.Formatter} containing one int
   *        argument for the shard number.
   * @param mimeType The string to be passed as the mimeType to GCS.
   */
  public GoogleCloudStorageLevelDbOutput(String bucket, String fileNamePattern, String mimeType,
      int shardCount) {
    output =
        new GoogleCloudStorageFileOutput(bucket, fileNamePattern, mimeType, shardCount);
  }

  @Override
  public List<? extends OutputWriter<ByteBuffer>> createWriters() {
    List<GoogleCloudStorageFileOutputWriter> writers = output.createWriters();
    List<GoogleCloudStorageLevelDbOutputWriter> result =
        new ArrayList<GoogleCloudStorageLevelDbOutputWriter>(writers.size());
    for (GoogleCloudStorageFileOutputWriter writer : writers) {
      result.add(new GoogleCloudStorageLevelDbOutputWriter(writer));
    }
    return result;
  }

  @Override
  public GoogleCloudStorageFileSet finish(Collection<? extends OutputWriter<ByteBuffer>> writers)
      throws IOException {
    ArrayList<OutputWriter<ByteBuffer>> wrapped =
        new ArrayList<OutputWriter<ByteBuffer>>(writers.size());
    for (OutputWriter<ByteBuffer> w : writers) {
      GoogleCloudStorageLevelDbOutputWriter writer = (GoogleCloudStorageLevelDbOutputWriter) w;
      wrapped.add(writer.getDelegate());
    }
    return output.finish(wrapped);
  }

  @Override
  public int getNumShards() {
    return output.getNumShards();
  }

}
