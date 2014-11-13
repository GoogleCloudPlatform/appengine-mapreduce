package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Sharder;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Writes {@link KeyValue}s out to a number of output writers each corresponding to a different
 * shard A sharder and an Output object are provided in the constructor. The output object will be
 * used to create the writers that will be written to. The sharder will determine which writer any
 * given record will be written to based on the key.
 *
 * @param <K> key type
 * @param <V> value type
 * @param <WriterT> type of the output writer being written to
 */
public abstract class ShardingOutputWriter<K, V, WriterT extends OutputWriter<KeyValue<K, V>>>
    extends OutputWriter<KeyValue<K, V>> {
  private static final long serialVersionUID = 1387792137406042414L;
  protected final Sharder sharder;
  private final Map<Integer, WriterT> writers = new HashMap<>();
  private final Marshaller<K> keyMarshaller;

  public ShardingOutputWriter(Marshaller<K> keyMarshaller, Sharder sharder) {
    this.keyMarshaller = Preconditions.checkNotNull(keyMarshaller);
    this.sharder = Preconditions.checkNotNull(sharder);
  }

  @Override
  public void beginShard() throws IOException {
    for (OutputWriter<KeyValue<K, V>> writer : writers.values()) {
      writer.beginShard();
    }
  }

  @Override
  public void endSlice() throws IOException {
    for (OutputWriter<KeyValue<K, V>> writer : writers.values()) {
      writer.endSlice();
    }
  }

  @Override
  public void beginSlice() throws IOException {
    for (OutputWriter<KeyValue<K, V>> writer : writers.values()) {
      writer.beginSlice();
    }
  }

  @Override
  public void write(KeyValue<K, V> value) throws IOException {
    ByteBuffer key = keyMarshaller.toBytes(value.getKey());
    int shard = sharder.getShardForKey(key);
    WriterT writer = writers.get(shard);
    if (writer == null) {
      writer = createWriter(shard);
      writers.put(shard, writer);
      writer.beginShard();
      writer.beginSlice();
    }
    writer.write(value);
  }

  /**
   * Creates a new writer. (This is called at most once per shard)
   *
   * @param shardId Implementations should not assume numbers provided are either in-order or
   *        continuous.
   */
  protected abstract WriterT createWriter(int shardId);

  @Override
  public void endShard() throws IOException {
    for (OutputWriter<KeyValue<K, V>> writer : writers.values()) {
      writer.endShard();
    }
  }

  @Override
  public abstract long estimateMemoryRequirement();

  protected Map<Integer, WriterT> getShardsToWriterMap() {
    return Collections.unmodifiableMap(writers);
  }

  @Override
  public boolean allowSliceRetry() {
    for (OutputWriter<KeyValue<K, V>> writer : writers.values()) {
      if (!writer.allowSliceRetry()) {
        return false;
      }
    }
    return true;
  }
}