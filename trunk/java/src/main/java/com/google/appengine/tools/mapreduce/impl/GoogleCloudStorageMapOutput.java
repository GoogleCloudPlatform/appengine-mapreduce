package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Sharder;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageLevelDbOutput;
import com.google.appengine.tools.mapreduce.outputs.MarshallingOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Defines the format the data is written out of from the Mapper. This consists of a number of files
 * in GCS where the content is split up by the provided {@link Sharder} each of these is written in
 * LevelDb Format and then using the {@link KeyValueMarshaller} to marshall the individual record.
 *
 *
 * @param <K> type of intermediate keys
 * @param <V> type of intermediate values
 */
public class GoogleCloudStorageMapOutput<K, V> extends
    Output<KeyValue<K, V>, List<GoogleCloudStorageFileSet>> {

  private static final long serialVersionUID = 1929076635709093020L;

  private static final String MIME_TYPE = MapReduceConstants.MAP_OUTPUT_MIME_TYPE;

  private final String mrJobId;

  private final String bucket;

  private final Marshaller<K> keyMarshaller;

  private final Marshaller<V> valueMarshaller;

  private final int mapShardCount;

  private final int reduceShardCount;

  private final Sharder sharder;

  public GoogleCloudStorageMapOutput(String bucket,
      String mrJobId,
      int mapShardCount,
      Marshaller<K> keyMarshaller,
      Marshaller<V> valueMarshaller,
      Sharder sharder) {
    this.bucket = checkNotNull(bucket, "Null bucket");
    this.sharder = checkNotNull(sharder, "Null sharder");
    this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
    this.mapShardCount = mapShardCount;
    checkArgument(mapShardCount >= 0);
    this.reduceShardCount = sharder.getNumShards();
    checkArgument(reduceShardCount >= 0);
    this.keyMarshaller = checkNotNull(keyMarshaller, "Null keyMarshaller");
    this.valueMarshaller = checkNotNull(valueMarshaller, "Null valueMarshaller");

  }

  @Override
  public List<? extends OutputWriter<KeyValue<K, V>>> createWriters() {
    List<ShardingWriter<K, V, GoogleCloudStorageFileSet>> result =
        new ArrayList<ShardingWriter<K, V, GoogleCloudStorageFileSet>>(mapShardCount);
    for (int i = 0; i < mapShardCount; i++) {
      String fileNamePattern = // Filled in later
          String.format(MapReduceConstants.MAP_OUTPUT_DIR_FORMAT, mrJobId, i);
      MarshallingOutput<KeyValue<K, V>, GoogleCloudStorageFileSet> output = new MarshallingOutput<
          KeyValue<K, V>, GoogleCloudStorageFileSet>(
          new GoogleCloudStorageLevelDbOutput(bucket, fileNamePattern, MIME_TYPE, reduceShardCount),
          new KeyValueMarshaller<K, V>(keyMarshaller, valueMarshaller));
      ShardingWriter<K, V, GoogleCloudStorageFileSet> shardingWriter =
          new ShardingWriter<K, V, GoogleCloudStorageFileSet>(keyMarshaller, sharder, output);
      result.add(shardingWriter);
    }
    return result;
  }

  @Override
  public List<GoogleCloudStorageFileSet> finish(
      Collection<? extends OutputWriter<KeyValue<K, V>>> writers) throws IOException {
    List<GoogleCloudStorageFileSet> result = new ArrayList<GoogleCloudStorageFileSet>();
    assert writers.size() == mapShardCount;
    for (OutputWriter<KeyValue<K, V>> writer : writers) {
      @SuppressWarnings("unchecked")
      ShardingWriter<K, V, GoogleCloudStorageFileSet> shardingWriter =
          (ShardingWriter<K, V, GoogleCloudStorageFileSet>) writer;
      GoogleCloudStorageFileSet files =
          shardingWriter.getOutput().finish(shardingWriter.getWriters());
      result.add(files);
    }
    return result;
  }

  @Override
  public int getNumShards() {
    return mapShardCount;
  }

}
