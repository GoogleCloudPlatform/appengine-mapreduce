// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.shardedjob.JobFailureException;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;

/**
 * An {@link Output} that throws an exception whenever an attempt is made to
 * write a value.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <O> type of output values formally (but not actually) accepted by this output
 * @param <R> type of result formally returned accepted by this output (though always return null)
 */
public class NoOutput<O, R> extends Output<O, R> {

  private static final long serialVersionUID = 965415182637510898L;

  private static class Writer<O> extends OutputWriter<O> {

    private static final long serialVersionUID = 524459343516880300L;

    @Override
    public void write(O object) {
      throw new JobFailureException("Attempt to write to NoOutput: " + object);
    }

    @Override
    public boolean allowSliceRetry() {
      return true;
    }
  }

  @Override
  public List<? extends OutputWriter<O>> createWriters(int numShards) {
    ImmutableList.Builder<Writer<O>> out = ImmutableList.builder();
    for (int i = 0; i < numShards; i++) {
      out.add(new Writer<O>());
    }
    return out.build();
  }

  /**
   * Returns {@code null}.
   */
  @Override
  public R finish(Collection<? extends OutputWriter<O>> writers) {
    return null;
  }
}
