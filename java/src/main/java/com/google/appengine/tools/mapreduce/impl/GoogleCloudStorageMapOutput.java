package com.google.appengine.tools.mapreduce.impl;


import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.MAP_OUTPUT_DIR_FORMAT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Sharder;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageMapOutputWriter.MapOutputWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

/**
 * An {@link Output} that is used for the map stage.
 * This consists of a number of GCS files where the content is split up by the provided
 * {@link Sharder}.
 *
 *
 * @param <K> type of intermediate keys
 * @param <V> type of intermediate values
 */
public class GoogleCloudStorageMapOutput<K, V> extends Output<KeyValue<K, V>, FilesByShard> {

  private static final long serialVersionUID = 7496044634366491296L;
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

  @Override
  public List<? extends OutputWriter<KeyValue<K, V>>> createWriters(int shards) {
    List<OutputWriter<KeyValue<K, V>>> result = new ArrayList<>(shards);
    for (int i = 0; i < shards; i++) {
      String fileNamePattern = String.format(MAP_OUTPUT_DIR_FORMAT, mrJobId, i);
      OutputWriter<KeyValue<K, V>> writer = new GoogleCloudStorageMapOutputWriter<>(
          bucket, fileNamePattern, keyMarshaller, valueMarshaller, sharder);
      result.add(writer);
    }
    return result;
  }

  @Override
  public FilesByShard finish(Collection<? extends OutputWriter<KeyValue<K, V>>> outputWriters)
      throws IOException {
    @SuppressWarnings("unchecked")
    Collection<GoogleCloudStorageMapOutputWriter<K, V>> writers =
        (Collection<GoogleCloudStorageMapOutputWriter<K, V>>) outputWriters;
    FilesByShard result = new FilesByShard(sharder.getNumShards(), bucket);
    for (GoogleCloudStorageMapOutputWriter<K, V> writer : writers) {
      for (Entry<Integer, MapOutputWriter<K, V>> e : writer.getShardsToWriterMap().entrySet()) {
        result.addFilesToShard(e.getKey(), e.getValue().getFiles());
      }
    }
    return result;
  }
}
