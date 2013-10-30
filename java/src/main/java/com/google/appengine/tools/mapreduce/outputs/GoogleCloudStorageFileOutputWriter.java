package com.google.appengine.tools.mapreduce.outputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsOutputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.mapreduce.LifecycleListenerRegistry;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Worker;
import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;
import com.google.common.base.Preconditions;

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

  private final GcsFilename file;
  private boolean closed = false;
  private GcsOutputChannel channel;
  private String mimeType;


  public GoogleCloudStorageFileOutputWriter(GcsFilename file, String mimeType) {
    this.mimeType = checkNotNull(mimeType, "Null mimeType");
    this.file = checkNotNull(file, "Null file");
  }

  public static GoogleCloudStorageFileOutputWriter forWorker(Worker<?> worker,
      String bucket, String fileName, String mimeType) {
    return forRegistry(worker.getLifecycleListenerRegistry(), bucket, fileName, mimeType);
  }

  @SuppressWarnings("unused")
  public static GoogleCloudStorageFileOutputWriter forRegistry(LifecycleListenerRegistry registry,
      String bucket, String fileName, String mimeType) {
    GcsFilename file = new GcsFilename(bucket, fileName);
    GoogleCloudStorageFileOutputWriter writer =
        new GoogleCloudStorageFileOutputWriter(file, mimeType);
    // We could now add a listener to registry but it so happens that we don't
    // currently care about {begin,end}{Slice,Shard}.
    return writer;
  }

  @Override
  public void write(ByteBuffer bytes) throws IOException {
    Preconditions.checkState(!closed, "%s: already closed", this);
    while (bytes.hasRemaining()) {
      channel.write(bytes);
    }
  }

  @Override
  public void open() throws IOException {
    channel =
        GCS_SERVICE.createOrReplace(file, new GcsFileOptions.Builder().mimeType(mimeType).build());
  }

  @Override
  public void endSlice() throws IOException {
    channel.waitForOutstandingWrites();
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    if (channel != null) {
      channel.close();
    }
    closed = true;
  }

  public GcsFilename getFile() {
    return file;
  }

  @Override
  public String toString() {
    return "CloudFileOutputWriter [file=" + file + ", closed=" + closed + "]";
  }

  @Override
  public long estimateMemoryRequirment() {
    return MapReduceConstants.DEFAULT_IO_BUFFER_SIZE * 2; // Double buffered
  }

}
