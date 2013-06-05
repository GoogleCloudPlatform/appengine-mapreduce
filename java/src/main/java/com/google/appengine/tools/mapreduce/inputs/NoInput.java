// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * An {@link Input} that does not produce any values.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <I> the type of input values formally (but not actually) produced by
 *            this input
 */
public class NoInput<I> extends Input<I> {
  private static final long serialVersionUID = 214109122708935335L;

  public static <I> NoInput<I> create(int numShards) {
    return new NoInput<I>(numShards);
  }

  private static class Reader<I> extends InputReader<I> {
    private static final long serialVersionUID = 171763263195134256L;

    @Override public Double getProgress() {
      return 1.0;
    }

    @Override public I next() {
      throw new NoSuchElementException();
    }
  }

  private final int numShards;

  public NoInput(int numShards) {
    this.numShards = numShards;
  }

  @Override public List<? extends InputReader<I>> createReaders() {
    ImmutableList.Builder<Reader<I>> out = ImmutableList.builder();
    for (int i = 0; i < numShards; i++) {
      out.add(new Reader<I>());
    }
    return out.build();
  }

}
