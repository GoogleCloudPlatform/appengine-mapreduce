// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
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

  @Override
  public List<? extends OutputWriter<O>> createWriters(int numShards) {
    ImmutableList.Builder<InMemoryOutputWriter<O>> out = ImmutableList.builder();
    for (int i = 0; i < numShards; i++) {
      out.add(new InMemoryOutputWriter<O>());
    }
    return out.build();
  }

  /**
   * Returns a list of lists where the outer list has one element for each
   * reduce shard, which is a list of the values emitted by that shard, in
   * order.
   */
  @Override
  public List<List<O>> finish(Collection<? extends OutputWriter<O>> writers) {
    ImmutableList.Builder<List<O>> out = ImmutableList.builder();
    for (OutputWriter<O> w : writers) {
      InMemoryOutputWriter<O> writer = (InMemoryOutputWriter<O>) w;
      out.add(ImmutableList.copyOf(writer.getResult()));
    }
    return out.build();
  }
}
