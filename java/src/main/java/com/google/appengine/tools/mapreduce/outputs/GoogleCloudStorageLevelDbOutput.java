package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;

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
   * Creates LevelDb output files who's names follow the provided pattern in the specified bucket.
   *
   * @param fileNamePattern a Java format string {@link java.util.Formatter} containing one int
   *        argument for the shard number.
   * @param mimeType The string to be passed as the mimeType to GCS.
   */
  public GoogleCloudStorageLevelDbOutput(String bucket, String fileNamePattern, String mimeType) {
    output = new GoogleCloudStorageFileOutput(bucket, fileNamePattern, mimeType);
  }

  @Override
  public List<GoogleCloudStorageLevelDbOutputWriter> createWriters(int numShards) {
    List<GoogleCloudStorageFileOutputWriter> writers = output.createWriters(numShards);
    List<GoogleCloudStorageLevelDbOutputWriter> result = new ArrayList<>(writers.size());
    for (GoogleCloudStorageFileOutputWriter writer : writers) {
      result.add(new GoogleCloudStorageLevelDbOutputWriter(writer));
    }
    return result;
  }

  @Override
  public GoogleCloudStorageFileSet finish(Collection<? extends OutputWriter<ByteBuffer>> writers) {
    ArrayList<OutputWriter<ByteBuffer>> wrapped = new ArrayList<>(writers.size());
    for (OutputWriter<ByteBuffer> w : writers) {
      GoogleCloudStorageLevelDbOutputWriter writer = (GoogleCloudStorageLevelDbOutputWriter) w;
      wrapped.add(writer.getDelegate());
    }
    return output.finish(wrapped);
  }
}
