// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.outputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * An {@link Output} that marshalls records.
 *
 * @param <O> type of values produced by this output
 * @param <R> type returned by {@link #finish} on the supplied sink
 */
public class MarshallingOutput<O, R> extends Output<O, R> {
  private static final long serialVersionUID = 184437617254585618L;

  private final Output<ByteBuffer, R> sink;
  private final Marshaller<O> marshaller;

  public MarshallingOutput(Output<ByteBuffer, R> sink, Marshaller<O> marshaller) {
    this.marshaller = checkNotNull(marshaller, "Null marshaller");
    this.sink = checkNotNull(sink, "Null sink");
  }

  @Override
  public List<MarshallingOutputWriter<O>> createWriters() {
    List<? extends OutputWriter<ByteBuffer>> writers = sink.createWriters();
    List<MarshallingOutputWriter<O>> result =
        new ArrayList<MarshallingOutputWriter<O>>(writers.size());
    for (OutputWriter<ByteBuffer> writer : writers) {
      result.add(new MarshallingOutputWriter<O>(writer, marshaller));
    }
    return result;
  }

  @Override
  public R finish(Collection<? extends OutputWriter<O>> writers) throws IOException {
    ArrayList<OutputWriter<ByteBuffer>> wrapped =
        new ArrayList<OutputWriter<ByteBuffer>>(writers.size());
    for (OutputWriter<O> w : writers) {
      @SuppressWarnings("unchecked")
      MarshallingOutputWriter<O> writer = (MarshallingOutputWriter<O>) w;
      wrapped.add(writer.getDelegate());
    }
    return sink.finish(wrapped);
  }

  @Override
  public int getNumShards() {
    return sink.getNumShards();
  }
}
