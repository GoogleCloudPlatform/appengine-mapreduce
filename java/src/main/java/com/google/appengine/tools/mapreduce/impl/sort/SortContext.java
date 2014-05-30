package com.google.appengine.tools.mapreduce.impl.sort;

import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.BaseShardContext;
import com.google.appengine.tools.mapreduce.impl.IncrementalTaskContext;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Provides a context for the in memory sort.
 *
 */
public class SortContext extends BaseShardContext<KeyValue<ByteBuffer, Iterator<ByteBuffer>>> {

  SortContext(IncrementalTaskContext c,
      OutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>> output) {
    super(c, output);
  }
}
