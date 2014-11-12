package com.google.appengine.tools.mapreduce.outputs;

import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.DEFAULT_IO_BUFFER_SIZE;
import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.GCS_RETRY_PARAMETERS;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import com.google.appengine.tools.cloudstorage.GcsOutputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.GcsServiceOptions;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An {@link OutputWriter} that writes bytes to a GCS file that it creates. Produces a single file
 * output (usually on a per-shard basis). This format does not insert any separator characters, so
 * it by default cannot be read back with the CloudStorageLineInputReader.
 *
 */
public class GoogleCloudStorageFileOutputWriter extends OutputWriter<ByteBuffer> {
  private static final long serialVersionUID = -4019473590179157706L;
  private static final Logger logger =
      Logger.getLogger(GoogleCloudStorageFileOutputWriter.class.getName());
  private static final GcsService GCS_SERVICE = GcsServiceFactory.createGcsService(
      new GcsServiceOptions.Builder()
          .setRetryParams(GCS_RETRY_PARAMETERS)
          .setDefaultWriteBufferSize(DEFAULT_IO_BUFFER_SIZE)
          .setHttpHeaders(ImmutableMap.of("User-Agent", "App Engine MR"))
          .build());
  private static final Random RND = new SecureRandom();

  public static final long MEMORY_REQUIRED_WITHOUT_SLICE_RETRY =
      MapReduceConstants.DEFAULT_IO_BUFFER_SIZE * 2;
  public static final long MEMORY_REQUIRED = MapReduceConstants.DEFAULT_IO_BUFFER_SIZE * 3;

  private final GcsFilename file;
  private final String mimeType;
  private final boolean supportSliceRetries;
  private GcsOutputChannel channel;
  private GcsOutputChannel sliceChannel;
  private final List<String> toDelete = new ArrayList<>();

  public GoogleCloudStorageFileOutputWriter(GcsFilename file, String mimeType) {
    this(file, mimeType, true);
  }

  public GoogleCloudStorageFileOutputWriter(GcsFilename file, String mimeType,
      boolean supportSliceRetries) {
    this.file = checkNotNull(file, "Null file");
    this.mimeType = checkNotNull(mimeType, "Null mimeType");
    this.supportSliceRetries = supportSliceRetries;
  }

  @Override
  public void cleanup() {
    for (String name : toDelete) {
      try {
        GCS_SERVICE.delete(new GcsFilename(file.getBucketName(), name));
      } catch (IOException ex) {
        logger.log(Level.WARNING, "Could not cleanup temporary file " + name, ex);
      }
    }
    toDelete.clear();
  }

  @Override
  public void beginShard() throws IOException {
    GcsFileOptions fileOptions = new GcsFileOptions.Builder().mimeType(mimeType).build();
    GcsFilename dest = new GcsFilename(file.getBucketName(), file.getObjectName() + "~");
    channel = GCS_SERVICE.createOrReplace(dest, fileOptions);
    sliceChannel = null;
    toDelete.clear();
  }

  @Override
  public void beginSlice() throws IOException {
    cleanup();
    if (supportSliceRetries) {
      if (sliceChannel != null) {
        copy(sliceChannel.getFilename(), channel);
        toDelete.add(sliceChannel.getFilename().getObjectName());
      }
      GcsFileOptions opt = new GcsFileOptions.Builder().mimeType(mimeType).build();
      String name = file.getObjectName() + "~" + Math.abs(RND.nextLong());
      sliceChannel = GCS_SERVICE.createOrReplace(new GcsFilename(file.getBucketName(), name), opt);
    } else {
      sliceChannel = channel;
    }
  }

  private static void copy(GcsFilename from, GcsOutputChannel toChannel) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(MapReduceConstants.DEFAULT_IO_BUFFER_SIZE);
    try (GcsInputChannel fromChannel = GCS_SERVICE.openReadChannel(from, 0)) {
      while (fromChannel.read(buffer) >= 0) {
        buffer.flip();
        while (buffer.hasRemaining()) {
          toChannel.write(buffer);
        }
        buffer.clear();
      }
    }
  }

  @Override
  public void write(ByteBuffer bytes) throws IOException {
    Preconditions.checkState(sliceChannel != null, "%s: channel was not created", this);
    while (bytes.hasRemaining()) {
      sliceChannel.write(bytes);
    }
  }

  @Override
  public void endSlice() throws IOException {
    if (supportSliceRetries) {
      sliceChannel.close();
    }
    channel.waitForOutstandingWrites();
  }

  @Override
  public void endShard() throws IOException {
    if (channel == null) {
      return;
    }
    channel.close();
    if (supportSliceRetries && sliceChannel != null) {
      // compose temporary destination and last slice to final destination
      List<String> source = ImmutableList.of(channel.getFilename().getObjectName(),
          sliceChannel.getFilename().getObjectName());
      GcsFileOptions opt = new GcsFileOptions.Builder().mimeType(mimeType).build();
      GCS_SERVICE.compose(source, file);
      GCS_SERVICE.update(file, opt);
      toDelete.add(sliceChannel.getFilename().getObjectName());
      sliceChannel = null;
    } else {
      // rename temporary destination to final destination
      GCS_SERVICE.copy(channel.getFilename(), file);
    }
    toDelete.add(channel.getFilename().getObjectName());
    channel = null;
  }

  public GcsFilename getFile() {
    return file;
  }

  @Override
  public String toString() {
    return "GoogleCloudStorageFileOutputWriter [file=" + file + ", mimeType=" + mimeType
        + ", channel=" + channel + ", supportSliceRetries= " + supportSliceRetries
        + ", sliceChannel=" + sliceChannel + ", toDelete=" + toDelete + "]";
  }

  @Override
  public long estimateMemoryRequirement() {
    return MEMORY_REQUIRED;
  }
}
