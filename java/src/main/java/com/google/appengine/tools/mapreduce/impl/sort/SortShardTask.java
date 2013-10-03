package com.google.appengine.tools.mapreduce.impl.sort;

import com.google.appengine.tools.mapreduce.CounterNames;
import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;
import com.google.appengine.tools.mapreduce.impl.WorkerShardTask;
import com.google.common.base.Charsets;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;

/**
 * Keeps accepting inputs and passing them to a SortWorker until the worker's buffer is full. Once
 * this occurs endSlice is called which causes the data to be written out in sorted order.
 *
 */
public class SortShardTask extends WorkerShardTask<
    KeyValue<ByteBuffer, ByteBuffer>, KeyValue<ByteBuffer, Iterator<ByteBuffer>>, SortContext> {
  private static final long serialVersionUID = -8041992113450564646L;

  private SortWorker inMemSorter;

  public SortShardTask(String mrJobId,
      int shardNumber,
      int shardCount,
      InputReader<KeyValue<ByteBuffer, ByteBuffer>> in,
      SortWorker worker,
      OutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>> out) {
    super(mrJobId,
        shardNumber,
        shardCount,
        in,
        worker,
        out,
        CounterNames.SORT_CALLS,
        CounterNames.SORT_WALLTIME_MILLIS);
    inMemSorter = worker;
  }

  @Override
  protected SortContext getWorkerContext(Counters counters) {
    return new SortContext(mrJobId, shardNumber, out, counters);
  }

  @Override
  protected void callWorker(KeyValue<ByteBuffer, ByteBuffer> input) {
    inMemSorter.addValue(input.getKey(), input.getValue());
  }

  @Override
  protected String formatLastWorkItem(KeyValue<ByteBuffer, ByteBuffer> item) {
    if (item == null) {
      return "null";
    }
    int maxSize = MapReduceConstants.MAX_LAST_ITEM_STRING_SIZE;
    Charset utf8 = Charsets.UTF_8;
    ByteBuffer value = item.getValue().slice();
    value.limit(value.position() + Math.min(maxSize, value.remaining()));
    CharBuffer string = utf8.decode(value);
    return "Key: " + utf8.decode(item.getKey()) + " Value: " + string.toString()
        + (item.getValue().remaining() >= maxSize ? " ..." : "");
  }

  @Override
  protected boolean shouldContinue() {
    return !inMemSorter.isFull();
  }

}
