package com.google.appengine.tools.mapreduce.impl.sort;

import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.WorkerContext;
import com.google.appengine.tools.mapreduce.impl.IncrementalTaskContext;
import com.google.appengine.tools.mapreduce.impl.BaseShardContext;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

/**
 * Provides a context for the in memory sort.
 *
 */
public class SortContext extends BaseShardContext
    implements WorkerContext<KeyValue<ByteBuffer, Iterator<ByteBuffer>>> {

  private final OutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>> output;

  SortContext(IncrementalTaskContext c,
      OutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>> output) {
    super(c);
    this.output = Preconditions.checkNotNull(output);
  }

  /**
   * Emits a list of values for a given key
   */
  public void emit(ByteBuffer key, List<ByteBuffer> values) throws IOException {
    emit(new KeyValue<>(key, values.iterator()));
  }

  @Override
  public void emit(KeyValue<ByteBuffer, Iterator<ByteBuffer>> value) throws IOException {
    output.write(value);
  }
}
