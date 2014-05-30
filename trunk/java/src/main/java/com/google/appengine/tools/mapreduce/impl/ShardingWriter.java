package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Sharder;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Writes {@link KeyValue}s out to a number of output writers each corresponding to a different
 * shard A sharder and an Output object are provided in the constructor. The output object will be
 * used to create the writers that will be written to. The sharder will determine which writer any
 * given record will be written to based on the key.
 *
 * @param <K> key type
 * @param <V> value type
 * @param <R> The result type of the provide output object. See:
 *        {@link Output#finish(java.util.Collection)}
 */
final class ShardingWriter<K, V, R> extends OutputWriter<KeyValue<K, V>> {

  private static final long serialVersionUID = 4472397467516370717L;
  private final Sharder sharder;
  private final List<? extends OutputWriter<KeyValue<K, V>>> writers;
  private final Output<KeyValue<K, V>, R> output;
  private final Marshaller<K> keyMarshaller;

  ShardingWriter(
      Marshaller<K> keyMarshaller, Sharder sharder,  Output<KeyValue<K, V>, R> output) {
    this.keyMarshaller = Preconditions.checkNotNull(keyMarshaller);
    this.sharder = Preconditions.checkNotNull(sharder);
    this.output = Preconditions.checkNotNull(output);
    this.writers = output.createWriters();
  }

  @Override
  public void beginShard() throws IOException {
    for (OutputWriter<KeyValue<K, V>> writer : writers) {
      writer.beginShard();
    }
  }

  @Override
  public void endSlice() throws IOException {
    for (OutputWriter<KeyValue<K, V>> writer : writers) {
      writer.endSlice();
    }
  }

  @Override
  public void beginSlice() throws IOException {
    for (OutputWriter<KeyValue<K, V>> writer : writers) {
      writer.beginSlice();
    }
  }

  @Override
  public void write(KeyValue<K, V> value) throws IOException {
    ByteBuffer key = keyMarshaller.toBytes(value.getKey());
    int shard = sharder.getShardForKey(key);
    OutputWriter<KeyValue<K, V>> writer = writers.get(shard);
    writer.write(value);
  }

  @Override
  public void endShard() throws IOException {
    for (OutputWriter<KeyValue<K, V>> writer : writers) {
      writer.endShard();
    }
  }

  List<? extends OutputWriter<KeyValue<K, V>>> getWriters() {
    return writers;
  }

  Output<KeyValue<K, V>, R> getOutput() {
    return output;
  }

  @Override
  public long estimateMemoryRequirement() {
    long total = 0;
    for (OutputWriter<KeyValue<K, V>> writer : writers) {
      total += writer.estimateMemoryRequirement();
    }
    return total;
  }

  @Override
  public boolean allowSliceRetry() {
    for (OutputWriter<KeyValue<K, V>> writer : writers) {
      if (!writer.allowSliceRetry()) {
        return false;
      }
    }
    return true;
  }
}