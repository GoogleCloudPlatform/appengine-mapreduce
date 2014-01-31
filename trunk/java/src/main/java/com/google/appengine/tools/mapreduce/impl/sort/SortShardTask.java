package com.google.appengine.tools.mapreduce.impl.sort;

import static com.google.appengine.tools.mapreduce.CounterNames.SORT_CALLS;
import static com.google.appengine.tools.mapreduce.CounterNames.SORT_WALLTIME_MILLIS;
import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.MAX_LAST_ITEM_STRING_SIZE;
import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.MAX_SORT_READ_TIME_MILLIS;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Worker;
import com.google.appengine.tools.mapreduce.impl.IncrementalTaskContext;
import com.google.appengine.tools.mapreduce.impl.RecoverableException;
import com.google.appengine.tools.mapreduce.impl.WorkerShardTask;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Iterator;

/**
 * Keeps accepting inputs and passing them to a SortWorker until the worker's buffer is full. Once
 * this occurs endSlice is called which causes the data to be written out in sorted order.
 *
 */
public class SortShardTask extends WorkerShardTask<
    KeyValue<ByteBuffer, ByteBuffer>, KeyValue<ByteBuffer, Iterator<ByteBuffer>>, SortContext> {

  private static final long serialVersionUID = -8041992113450564646L;

  private final SortWorker inMemSorter;
  private final InputReader<KeyValue<ByteBuffer, ByteBuffer>> in;
  private final OutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>> out;
  private final IncrementalTaskContext context;

  public SortShardTask(String mrJobId,
      int shardNumber,
      int shardCount,
      InputReader<KeyValue<ByteBuffer, ByteBuffer>> in,
      SortWorker worker,
      OutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>> out) {
    this.in = checkNotNull(in, "Null in");
    this.out = checkNotNull(out, "Null out");
    this.inMemSorter = worker;
    this.context = new IncrementalTaskContext(mrJobId, shardNumber, shardCount, SORT_CALLS,
        SORT_WALLTIME_MILLIS);
  }

  @Override
  public void prepare() {
    super.prepare();
    boolean success = false;
    try {
      inMemSorter.prepare();
      success = true;
    } finally {
      if (!success) {
        super.release();
      }
    }
  }

  @Override
  public void release() {
    super.release();
    inMemSorter.release();
  }

  @Override
  protected void callWorker(KeyValue<ByteBuffer, ByteBuffer> input) {
    try {
      inMemSorter.addValue(input.getKey(), input.getValue());
    } catch (Exception ex) {
      throw new RecoverableException("sort worker failure", ex);
    }
  }

  @Override
  protected String formatLastWorkItem(KeyValue<ByteBuffer, ByteBuffer> item) {
    if (item == null) {
      return "null";
    }
    ByteBuffer value = item.getValue().slice();
    value.limit(value.position() + Math.min(MAX_LAST_ITEM_STRING_SIZE, value.remaining()));
    CharBuffer string = UTF_8.decode(value);
    return "Key: " + UTF_8.decode(item.getKey()) + " Value: " + string.toString()
        + (item.getValue().remaining() >= MAX_LAST_ITEM_STRING_SIZE ? " ..." : "");
  }

  @Override
  protected boolean shouldCheckpoint(long timeElapsed) {
    return timeElapsed >= MAX_SORT_READ_TIME_MILLIS || inMemSorter.isFull();
  }

  @Override
  protected long estimateMemoryRequirement() {
    return in.estimateMemoryRequirement() + out.estimateMemoryRequirement() +
        inMemSorter.estimateMemoryRequirement();
  }

  @Override
  protected Worker<SortContext> getWorker() {
    return inMemSorter;
  }

  @Override
  public OutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>> getOutputWriter() {
    return out;
  }

  @Override
  protected void setContextOnWorker() {
    inMemSorter.setContext(new SortContext(context, out));
  }

  @Override
  public InputReader<KeyValue<ByteBuffer, ByteBuffer>> getInputReader() {
    return in;
  }

  @Override
  public IncrementalTaskContext getContext() {
    return context;
  }
}
