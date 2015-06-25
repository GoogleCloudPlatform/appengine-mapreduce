package com.google.appengine.tools.mapreduce.impl;

import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.DEFAULT_IO_BUFFER_SIZE;
import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.GCS_RETRY_PARAMETERS;
import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.MAP_OUTPUT_MIME_TYPE;
import static com.google.common.base.Preconditions.checkNotNull;

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
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
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

  private static final long serialVersionUID = 739934506831898405L;
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

    private static final long serialVersionUID = 6056683766896574858L;
    private final GcsFileOutputWriter gcsWriter;

    public MapOutputWriter(GcsFileOutputWriter gcsWriter, KeyValueMarshaller<K, V> marshaller) {
      super(new LevelDbOutputWriter(gcsWriter), marshaller);
      this.gcsWriter = gcsWriter;
    }

    Iterable<String> getFiles() {
      return gcsWriter.getFiles();
    }
  }

  private static class GcsFileOutputWriter extends OutputWriter<ByteBuffer> {

    private static final long serialVersionUID = 1864188391798776210L;
    private static final String MAX_COMPONENTS_PER_COMPOSE = "com.google.appengine.tools.mapreduce"
        + ".impl.GoogleCloudStorageMapOutputWriter.MAX_COMPONENTS_PER_COMPOSE";
    private static final String MAX_FILES_PER_COMPOSE = "com.google.appengine.tools.mapreduce.impl"
        + ".GoogleCloudStorageMapOutputWriter.MAX_FILES_PER_COMPOSE";
    private static final GcsService GCS_SERVICE = GcsServiceFactory.createGcsService(
        new GcsServiceOptions.Builder()
        .setRetryParams(GCS_RETRY_PARAMETERS)
        .setDefaultWriteBufferSize(DEFAULT_IO_BUFFER_SIZE)
        .setHttpHeaders(ImmutableMap.of("User-Agent", "App Engine MR"))
        .build());
    private static final GcsFileOptions FILE_OPTIONS =
        new GcsFileOptions.Builder().mimeType(MAP_OUTPUT_MIME_TYPE).build();
    private static final long MEMORY_REQUIRED = MapReduceConstants.DEFAULT_IO_BUFFER_SIZE * 2;


    private final int maxComponentsPerCompose;
    private final int maxFilesPerCompose;
    private final String bucket;
    private final String namePrefix;
    private final Set<String> toDelete = new HashSet<>();
    private final Set<String> sliceParts = new LinkedHashSet();
    private final Set<String> compositeParts = new LinkedHashSet<>();
    private String filePrefix;
    private int fileCount;

    private transient GcsOutputChannel channel;

    public GcsFileOutputWriter(String bucket, String namePrefix) {
      this.bucket = bucket;
      this.namePrefix = namePrefix;
      int temp = Integer.parseInt(System.getProperty(MAX_COMPONENTS_PER_COMPOSE, "1024"));
      maxFilesPerCompose = Integer.parseInt(System.getProperty(MAX_FILES_PER_COMPOSE, "32"));
      maxComponentsPerCompose = (temp / maxFilesPerCompose) * maxFilesPerCompose;
      Preconditions.checkArgument(maxFilesPerCompose > 0);
      Preconditions.checkArgument(maxComponentsPerCompose > 0);
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
      toDelete.addAll(sliceParts);
      toDelete.addAll(compositeParts);
      cleanup();
      sliceParts.clear();
      compositeParts.clear();
      fileCount = 0;
      channel = null;
      filePrefix = namePrefix + "-" + new Random().nextLong();
    }

    @Override
    public void beginSlice() throws IOException {
      cleanup();
      if (sliceParts.size() >= maxFilesPerCompose) {
        String tempFile = generateTempFileName();
        compose(sliceParts, tempFile);
        compositeParts.add(tempFile);
      }
      if (compositeParts.size() * maxFilesPerCompose >= maxComponentsPerCompose) {
        compose(compositeParts, getFileName(fileCount++));
      }
    }

    @Override
    public void write(ByteBuffer bytes) throws IOException {
      if (channel == null) {
        GcsFilename sliceFile = new GcsFilename(bucket, generateTempFileName());
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
        sliceParts.add(channel.getFilename().getObjectName());
        channel = null;
      }
    }

    @Override
    public void endShard() throws IOException {
      if (!sliceParts.isEmpty()) {
        String tempFile = generateTempFileName();
        compose(sliceParts, tempFile);
        compositeParts.add(tempFile);
      }
      if (!compositeParts.isEmpty()) {
        compose(compositeParts, getFileName(fileCount++));
      }
    }

    private String generateTempFileName() {
      while (true) {
        String tempFileName = filePrefix + "-" + new Random().nextLong();
        if (!sliceParts.contains(tempFileName) && !compositeParts.contains(tempFileName)) {
          return tempFileName;
        }
      }
    }

    private String getFileName(int part) {
      return filePrefix + "@" + part;
    }

    private void compose(Collection<String> source, String target) throws IOException {
      GcsFilename targetFile = new GcsFilename(bucket, target);
      if (source.size() == 1) {
        GCS_SERVICE.copy(new GcsFilename(bucket, source.iterator().next()), targetFile);
      } else {
        GCS_SERVICE.compose(source, targetFile);
      }
      for (String name : source) {
        if (!name.equals(target)) {
          toDelete.add(name);
        }
      }
      source.clear();
    }

    private Iterable<String> getFiles() {
      return new Iterable<String>() {
        @Override public Iterator<String> iterator() {
          return new AbstractIterator<String>() {
            private int index = 0;

            @Override
            protected String computeNext() {
              if (index < fileCount) {
                return getFileName(index++);
              }
              return endOfData();
            }
          };
        }
      };
    }

    @Override
    public String toString() {
      return getClass().getSimpleName()
          + " [bucket=" + bucket + ", maxComponentsPerCompose=" + maxComponentsPerCompose
          + ", maxFilesPerCompose=" + maxFilesPerCompose + ", namePrefix=" + namePrefix
          + ", filePrefix=" + filePrefix + ", fileCount=" + fileCount
          + ", toDelete=" + toDelete + ", sliceParts=" + sliceParts
          + ", compositeParts=" + compositeParts + "]";
    }
  }
}
