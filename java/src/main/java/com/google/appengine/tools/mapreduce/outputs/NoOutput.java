// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * An {@link Output} that throws an exception whenever an attempt is made to
 * write a value.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <O> type of output values formally (but not actually) accepted by this
 *            output
 * @param <R> type of result formally returned accepted by this output
 *            (but it actually returns null)
 */
public class NoOutput<O, R> extends Output<O, R> {
  private static final long serialVersionUID = 965415182637510898L;

  public static <O, R> NoOutput<O, R> create(int numShards) {
    return new NoOutput<O, R>(numShards);
  }

  private static class Writer<O> extends OutputWriter<O> {
    private static final long serialVersionUID = 524459343516880300L;

    @Override public void write(O object) {
      // TODO(ohler): Make this an exception that immediately aborts the entire
      // MR rather than causing a retry.
      throw new RuntimeException("Attempt to write to NoOutput: " + object);
    }

    @Override public void close() {
      // nothing
    }
  }

  private final int numShards;

  private NoOutput(int numShards) {
    this.numShards = numShards;
  }

  @Override public List<? extends OutputWriter<O>> createWriters() {
    ImmutableList.Builder<Writer<O>> out = ImmutableList.builder();
    for (int i = 0; i < numShards; i++) {
      out.add(new Writer<O>());
    }
    return out.build();
  }

  /**
   * Returns null.
   */
  @Override public R finish(List<? extends OutputWriter<O>> writers) {
    return null;
  }

}
