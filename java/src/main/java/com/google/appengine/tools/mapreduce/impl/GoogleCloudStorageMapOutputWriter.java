package com.google.appengine.tools.mapreduce.impl;

import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.DEFAULT_IO_BUFFER_SIZE;
import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.GCS_RETRY_PARAMETERS;
import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.MAP_OUTPUT_MIME_TYPE;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.repackaged.com.google.common.collect.ImmutableList;
import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsOutputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.GcsServiceOptions;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Sharder;
import com.google.appengine.tools.mapreduce.outputs.LevelDbOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.MarshallingOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.ShardingOutputWriter;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An {@link OutputWriter} that is used by the map stage and writes bytes to GCS,
 * later to be used by the sort/merge stages.
 * Content is written in a LevelDb log Format and then using the {@link KeyValueMarshaller}
 * to marshall the individual record.
 *
 *
 * @param <K> type of intermediate keys
 * @param <V> type of intermediate values
 */
public class GoogleCloudStorageMapOutputWriter<K, V>
    extends ShardingOutputWriter<K, V, GoogleCloudStorageMapOutputWriter.MapOutputWriter<K, V>> {

  private static final long serialVersionUID = -1989859043333177195L;
  private static final Logger logger =
      Logger.getLogger(GoogleCloudStorageMapOutputWriter.class.getName());

  private final String fileNamePattern;
  private final String bucket;
  private final KeyValueMarshaller<K, V> keyValueMarshaller;

  public GoogleCloudStorageMapOutputWriter(String bucket, String fileNamePattern,
      Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller, Sharder sharder) {
    super(keyMarshaller, sharder);
    this.bucket =  checkNotNull(bucket, "Null bucket");
    this.fileNamePattern = checkNotNull(fileNamePattern, "Null fileNamePattern");
    keyValueMarshaller = new KeyValueMarshaller<>(keyMarshaller, valueMarshaller);
  }

  @Override
  public boolean allowSliceRetry() {
    return true;
  }

  @Override
  public MapOutputWriter<K, V> createWriter(int sortShard) {
    String namePrefix = String.format(fileNamePattern, sortShard);
    return new MapOutputWriter<>(new GcsFileOutputWriter(bucket, namePrefix), keyValueMarshaller);
  }

  @Override
  protected Map<Integer, MapOutputWriter<K, V>> getShardsToWriterMap() {
    return super.getShardsToWriterMap();
  }

  @Override
  public long estimateMemoryRequirement() {
    return sharder.getNumShards() * GcsFileOutputWriter.MEMORY_REQUIRED;
  }

  static class MapOutputWriter<K, V> extends MarshallingOutputWriter<KeyValue<K, V>> {

    private static final long serialVersionUID = 1211941590518570124L;
    private final GcsFileOutputWriter gcsWriter;

    public MapOutputWriter(GcsFileOutputWriter gcsWriter, KeyValueMarshaller<K, V> marshaller) {
      super(new LevelDbOutputWriter(gcsWriter), marshaller);
      this.gcsWriter = gcsWriter;
    }

    List<String> getFiles() {
      return gcsWriter.getFiles();
    }
  }

  private static class GcsFileOutputWriter extends OutputWriter<ByteBuffer> {

    private static final long serialVersionUID = -4681879483578959369L;
    private static final int MAX_FILES_PER_COMPOSE = Integer.parseInt(
        System.getProperty("com.google.appengine.tools.mapreduce.impl"
            + ".GoogleCloudStorageMapOutputWriter.MAX_FILES_PER_COMPOSE",
            "1024"));
    private static final String FILENAME_FORMAT = "%s@%d";
    private static final String TEMP_FILENAME_FORMAT = "%s@%d~%d";
    private static final GcsService GCS_SERVICE = GcsServiceFactory.createGcsService(
        new GcsServiceOptions.Builder()
        .setRetryParams(GCS_RETRY_PARAMETERS)
        .setDefaultWriteBufferSize(DEFAULT_IO_BUFFER_SIZE)
        // TODO(user): include version once b/12689661 is fixed
        .setHttpHeaders(ImmutableMap.of("User-Agent", "App Engine MR"))
        .build());
    private static final GcsFileOptions FILE_OPTIONS =
        new GcsFileOptions.Builder().mimeType(MAP_OUTPUT_MIME_TYPE).build();
    private static final long MEMORY_REQUIRED = MapReduceConstants.DEFAULT_IO_BUFFER_SIZE * 2;

    private final String bucket;
    private final String namePrefix;
    private final List<String> toDelete = new ArrayList<>();
    private String previousSliceFileName;
    private int compositionCount;
    private int fileCount;
    private transient GcsOutputChannel channel;

    public GcsFileOutputWriter(String bucket, String namePrefix) {
      this.bucket = bucket;
      this.namePrefix = namePrefix;
    }

    @Override
    public void cleanup() {
      for (String name : toDelete) {
        try {
          GCS_SERVICE.delete(new GcsFilename(bucket, name));
        } catch (IOException ex) {
          logger.log(Level.WARNING, "Could not cleanup temporary file " + name, ex);
        }
      }
      toDelete.clear();
    }

    @Override
    public void beginShard() {
      cleanup();
      previousSliceFileName = null;
      compositionCount = 0;
      fileCount = 0;
      channel = null;
    }

    @Override
    public void beginSlice() throws IOException {
      cleanup();
      if (compositionCount >= MAX_FILES_PER_COMPOSE) {
        // Rename temp file (remove the _timestamp suffix).
        compose(ImmutableList.of(previousSliceFileName), getFileName(fileCount++));
        previousSliceFileName = null;
        compositionCount = 0;
      }
    }

    @Override
    public void write(ByteBuffer bytes) throws IOException {
      if (channel == null) {
        GcsFilename sliceFile = new GcsFilename(bucket, getTempFileName());
        channel = GCS_SERVICE.createOrReplace(sliceFile, FILE_OPTIONS);
      }
      while (bytes.hasRemaining()) {
        channel.write(bytes);
      }
    }

    @Override
    public void endSlice() throws IOException {
      if (channel != null) {
        channel.close();
        String fileName = channel.getFilename().getObjectName();
        if (previousSliceFileName != null) {
          compose(ImmutableList.of(previousSliceFileName, fileName), fileName);
          ++compositionCount;
        } else {
          compositionCount = 1;
        }
        previousSliceFileName = fileName;
        channel = null;
      }
    }

    private String getTempFileName() {
      return String.format(TEMP_FILENAME_FORMAT, namePrefix, fileCount, new Date().getTime());
    }

    private String getFileName(int idx) {
      return String.format(FILENAME_FORMAT, namePrefix, idx);
    }

    private void compose(List<String> source, String dest) throws IOException {
      if (source.size() == 1) {
        GCS_SERVICE.copy(new GcsFilename(bucket, source.get(0)), new GcsFilename(bucket, dest));
      } else {
        GCS_SERVICE.compose(source, new GcsFilename(bucket, dest));
      }
      for (String name : source) {
        if (!name.equals(dest)) {
          toDelete.add(name);
        }
      }
    }

    private List<String> getFiles() {
      List<String> files = new ArrayList<>(fileCount);
      for (int i = 0; i < fileCount; i++) {
        files.add(getFileName(i));
      }
      if (previousSliceFileName != null) {
        files.add(previousSliceFileName);
      }
      return files;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName()
          + " [bucket=" + bucket + ", namePrefix=" + namePrefix + ", toDelete=" + toDelete
          + ", previousSliceFileName=" + previousSliceFileName
          + ", compositionCount=" + compositionCount + ", fileCount= " + fileCount + "]";
    }
  }
}
