// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * An {@link Output} that collects all values in memory and returns them on
 * {@link #finish}.  Don't use this unless the entire output is small enough to
 * fit in memory.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <O> type of values accepted by this output
 */
public class InMemoryOutput<O> extends Output<O, List<List<O>>> {
  private static final long serialVersionUID = 184437617254585618L;

  public static <O> InMemoryOutput<O> create(int numShards) {
    return new InMemoryOutput<O>(numShards);
  }

  private static class Writer<O> extends OutputWriter<O> {
    private static final long serialVersionUID = 528522943983621278L;

    private boolean closed = false;
    private final List<O> accu = Lists.newArrayList();

    @Override public String toString() {
      return "InMemoryOutput.Writer(" + accu.size() + " items"
          + (closed ? ", closed" : " so far") + ")";
    }

    @Override public void write(O value) throws IOException {
      Preconditions.checkState(!closed, "%s: Already closed", this);
      accu.add(value);
    }

    @Override public void close() throws IOException {
      closed = true;
    }
  }

  private final int shardCount;

  public InMemoryOutput(int shardCount) {
    Preconditions.checkArgument(shardCount >= 0, "Negative shardCount: %s", shardCount);
    this.shardCount = shardCount;
  }

  @Override public List<? extends OutputWriter<O>> createWriters() {
    ImmutableList.Builder<Writer<O>> out = ImmutableList.builder();
    for (int i = 0; i < shardCount; i++) {
      out.add(new Writer<O>());
    }
    return out.build();
  }

  /**
   * Returns a list of lists where the outer list has one element for each
   * reduce shard, which is a list of the values emitted by that shard, in
   * order.
   */
  @Override public List<List<O>> finish(List<? extends OutputWriter<O>> writers) {
    ImmutableList.Builder<List<O>> out = ImmutableList.builder();
    for (OutputWriter<O> w : writers) {
      Writer<O> writer = (Writer<O>) w;
      out.add(ImmutableList.copyOf(writer.accu));
    }
    return out.build();
  }

}
