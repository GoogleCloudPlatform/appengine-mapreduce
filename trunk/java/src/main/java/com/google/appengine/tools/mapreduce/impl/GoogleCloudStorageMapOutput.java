package com.google.appengine.tools.mapreduce.impl;

import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.MAP_OUTPUT_MIME_TYPE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Sharder;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageLevelDbOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.MarshallingOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.ShardingOutputWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Defines the format the data is written out of from the Mapper. This consists of a number of files
 * in GCS where the content is split up by the provided {@link Sharder} each of these is written in
 * LevelDb Format and then using the {@link KeyValueMarshaller} to marshall the individual record.
 *
 *
 * @param <K> type of intermediate keys
 * @param <V> type of intermediate values
 */
public class GoogleCloudStorageMapOutput<K, V> extends Output<KeyValue<K, V>, FilesByShard> {

  private static final long serialVersionUID = 1929076635709093020L;

  private final String mrJobId;
  private final String bucket;
  private final Marshaller<K> keyMarshaller;
  private final Marshaller<V> valueMarshaller;

  private final Sharder sharder;

  public GoogleCloudStorageMapOutput(String bucket, String mrJobId, Marshaller<K> keyMarshaller,
      Marshaller<V> valueMarshaller, Sharder sharder) {
    this.bucket = checkNotNull(bucket, "Null bucket");
    this.sharder = checkNotNull(sharder, "Null sharder");
    this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
    checkArgument(sharder.getNumShards() >= 0);
    this.keyMarshaller = checkNotNull(keyMarshaller, "Null keyMarshaller");
    this.valueMarshaller = checkNotNull(valueMarshaller, "Null valueMarshaller");
  }

  private static class ShardingOutputWriterImpl<K, V> extends
      ShardingOutputWriter<K, V, OutputWriter<KeyValue<K, V>>> {

    private static final long serialVersionUID = 1674046447114388281L;
    private final String fileNamePattern;
    private final String bucket;
    private final Marshaller<K> keyMarshaller;
    private final Marshaller<V> valueMarshaller;

    public ShardingOutputWriterImpl(String bucket, String fileNamePattern,
        Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller, Sharder sharder) {
      super(keyMarshaller, sharder);
      this.bucket = bucket;
      this.fileNamePattern = fileNamePattern;
      this.keyMarshaller = keyMarshaller;
      this.valueMarshaller = valueMarshaller;
    }

    @Override
    public OutputWriter<KeyValue<K, V>> createWriter(int sortShard) {
      GcsFilename file = new GcsFilename(bucket, getFileName(sortShard));
      MarshallingOutputWriter<KeyValue<K, V>> output =
          new MarshallingOutputWriter<>(new GoogleCloudStorageLevelDbOutputWriter(
              new GoogleCloudStorageFileOutputWriter(file, MAP_OUTPUT_MIME_TYPE)),
              new KeyValueMarshaller<>(keyMarshaller, valueMarshaller));
      return output;
    }

    @Override
    protected Map<Integer, OutputWriter<KeyValue<K, V>>> getShardsToWriterMap() {
      return super.getShardsToWriterMap();
    }

    @Override
    public long estimateMemoryRequirement() {
      return sharder.getNumShards() * GoogleCloudStorageFileOutputWriter.MEMORY_REQUIRED;
    }

    private String getFileName(int sortShard) {
      return String.format(fileNamePattern, sortShard);
    }
  }

  @Override
  public List<? extends OutputWriter<KeyValue<K, V>>> createWriters(int shards) {
    List<ShardingOutputWriterImpl<K, V>> result = new ArrayList<>(shards);
    for (int i = 0; i < shards; i++) {
      String fileNamePattern = // Filled in later
          String.format(MapReduceConstants.MAP_OUTPUT_DIR_FORMAT, mrJobId, i);
      ShardingOutputWriterImpl<K, V> shardingWriter = new ShardingOutputWriterImpl<>(bucket,
          fileNamePattern, keyMarshaller, valueMarshaller, sharder);
      result.add(shardingWriter);
    }
    return result;
  }

  @Override
  public FilesByShard finish(Collection<? extends OutputWriter<KeyValue<K, V>>> writers)
      throws IOException {
    FilesByShard result = new FilesByShard(sharder.getNumShards(), bucket);
    for (OutputWriter<KeyValue<K, V>> writer : writers) {
      @SuppressWarnings("unchecked")
      ShardingOutputWriterImpl<K, V> shardingWriter = (ShardingOutputWriterImpl<K, V>) writer;
      for (int sortShard : shardingWriter.getShardsToWriterMap().keySet()) {
        result.addFileToShard(sortShard, shardingWriter.getFileName(sortShard));
      }
    }
    return result;
  }
}
