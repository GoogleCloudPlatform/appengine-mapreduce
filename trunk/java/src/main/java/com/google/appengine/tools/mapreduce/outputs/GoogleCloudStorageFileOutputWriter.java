package com.google.appengine.tools.mapreduce.outputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsOutputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An {@link OutputWriter} that writes bytes to a GCS file that it creates.
 * Produces a single file output (usually on a per-shard basis).
 * This format does not insert any separator characters, so it by default
 * cannot be read back with the CloudStorageLineInputReader.
 *
 */
public class GoogleCloudStorageFileOutputWriter extends OutputWriter<ByteBuffer> {
  private static final long serialVersionUID = -4019473590179157706L;

  private static final GcsService GCS_SERVICE =
      GcsServiceFactory.createGcsService(MapReduceConstants.GCS_RETRY_PARAMETERS);

  public static final long MEMORY_REQUIRED = MapReduceConstants.DEFAULT_IO_BUFFER_SIZE * 2;

  static {
    // TODO(user): include version once b/12689661 is fixed
    GCS_SERVICE.setHttpHeaders(ImmutableMap.of("User-Agent", "App Engine MR"));
  }

  private final GcsFilename file;
  private final String mimeType;
  private GcsOutputChannel channel;


  public GoogleCloudStorageFileOutputWriter(GcsFilename file, String mimeType) {
    this.file = checkNotNull(file, "Null file");
    this.mimeType = checkNotNull(mimeType, "Null mimeType");
  }

  @Override
  public void write(ByteBuffer bytes) throws IOException {
    Preconditions.checkState(channel != null, "%s: channel was not created", this);
    while (bytes.hasRemaining()) {
      channel.write(bytes);
    }
  }

  @Override
  public void beginShard() throws IOException {
    GcsFileOptions fileOptions = new GcsFileOptions.Builder().mimeType(mimeType).build();
    channel = GCS_SERVICE.createOrReplace(file, fileOptions);
  }

  @Override
  public void endSlice() throws IOException {
    channel.waitForOutstandingWrites();
  }

  @Override
  public void endShard() throws IOException {
    if (channel != null) {
      channel.close();
    }
    channel = null;
  }

  public GcsFilename getFile() {
    return file;
  }

  @Override
  public String toString() {
    return "CloudFileOutputWriter [file=" + file + ", mimeType=" + mimeType + ", channel="
        + channel + "]";
  }

  @Override
  public long estimateMemoryRequirement() {
    return MEMORY_REQUIRED;
  }
}
