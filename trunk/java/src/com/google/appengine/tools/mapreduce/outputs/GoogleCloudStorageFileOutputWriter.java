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

  private static final GcsService GCS_SERVICE = GcsServiceFactory.createGcsService();

  private final GcsFilename file;
  private boolean closed = false;
  private final GcsOutputChannel channel;


  GoogleCloudStorageFileOutputWriter(GcsFilename file, String mimeType) throws IOException {
    this.file = checkNotNull(file, "Null file");
    checkNotNull(mimeType, "Null mimeType");
    this.channel =
        GCS_SERVICE.createOrReplace(file, GcsFileOptions.builder().withMimeType(mimeType));
  }

  public static GoogleCloudStorageFileOutputWriter forWorker(Worker<?> worker,
      String bucket, String fileName, String mimeType) throws IOException {
    return forRegistry(worker.getLifecycleListenerRegistry(), bucket, fileName, mimeType);
  }

  public static GoogleCloudStorageFileOutputWriter forRegistry(LifecycleListenerRegistry registry,
      String bucket, String fileName, String mimeType) throws IOException {
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
    if (bytes.hasRemaining()) {
      channel.write(bytes);
    }
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

}
