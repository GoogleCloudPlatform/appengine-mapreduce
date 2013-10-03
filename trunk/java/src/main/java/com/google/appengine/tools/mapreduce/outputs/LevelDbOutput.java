// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.outputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.inputs.GoogleCloudStorageLevelDbInput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * An {@link Output} that writes LevelDb records.
 * Data written with this class can be read with {@link GoogleCloudStorageLevelDbInput}.
 *
 * @param <R> type returned by {@link #finish}
 */
public class LevelDbOutput<R> extends Output<ByteBuffer, R> {
  private static final long serialVersionUID = 184437617254585618L;

  private final Output<ByteBuffer, R> sink;

  /**
   * @param sink The output where data shoud be written.
   */
  public LevelDbOutput(Output<ByteBuffer, R> sink) {
    this.sink = checkNotNull(sink, "Null sink");
  }

  @Override
  public List<LevelDbOutputWriter> createWriters() {
    List<? extends OutputWriter<ByteBuffer>> writers = sink.createWriters();
    List<LevelDbOutputWriter> result = new ArrayList<LevelDbOutputWriter>(writers.size());
    for (OutputWriter<ByteBuffer> writer : writers) {
      result.add(new LevelDbOutputWriter(writer));
    }
    return result;
  }

  @Override
  public R finish(Collection<? extends OutputWriter<ByteBuffer>> writers) throws IOException {
    ArrayList<OutputWriter<ByteBuffer>> wrapped =
        new ArrayList<OutputWriter<ByteBuffer>>(writers.size());
    for (OutputWriter<ByteBuffer> w : writers) {
      @SuppressWarnings("unchecked")
      LevelDbOutputWriter writer = (LevelDbOutputWriter) w;
      wrapped.add(writer.getDelegate());
    }
    return sink.finish(wrapped);
  }

  @Override
  public int getNumShards() {
    return sink.getNumShards();
  }

}
