package com.google.appengine.tools.mapreduce.impl.sort;

import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.BaseShardContext;
import com.google.appengine.tools.mapreduce.impl.IncrementalTaskContext;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Provides a context for merging.
 *
 */
public class MergeContext extends BaseShardContext<KeyValue<ByteBuffer, List<ByteBuffer>>> {

  MergeContext(IncrementalTaskContext c,
      OutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>> output) {
    super(c, output);
  }
}
