// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class RandomLongInput extends Input<Long> {
  private static final long serialVersionUID = 524476737411668844L;

  private static class Reader extends InputReader<Long> {
    private static final long serialVersionUID = 764351972869495917L;

    private Random random;
    private long emitted = 0;
    private long toEmit;
    private long seed;

    private Reader(long seed, long toEmit) {
      this.seed = seed;
      this.toEmit = toEmit;
      random = new Random(seed);
    }

    @Override public Double getProgress() {
      return toEmit <= 0 ? 1 : emitted / (double) toEmit;
    }

    @Override public Long next() {
      if (emitted >= toEmit) {
        throw new NoSuchElementException();
      }
      emitted++;
      return random.nextLong();
    }

    @Override
    public void open() throws IOException {
      random = new Random(seed);
      emitted = 0;
    }
  }

  private final long valuesTotal;
  private final int shardCount;
  private long seed;

  public RandomLongInput(long valuesTotal, int shardCount) {
    Preconditions.checkArgument(valuesTotal >= 0, "valuesTotal should be >=0: %s", valuesTotal);
    Preconditions.checkArgument(shardCount > 0, "shardCount should be >0: %s", shardCount);
    this.valuesTotal = valuesTotal;
    this.shardCount = shardCount;
    this.seed = System.currentTimeMillis();
  }

  public RandomLongInput setSeed(long seed) {
    this.seed = seed;
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + valuesTotal + ", " + shardCount + ")";
  }

  @Override
  public List<? extends InputReader<Long>> createReaders() {
    ImmutableList.Builder<InputReader<Long>> b = ImmutableList.builder();
    long valuesPerShard = valuesTotal / shardCount;
    long remainder = valuesTotal % shardCount;
    for (int i = 0; i < shardCount; i++) {
      long countHere = valuesPerShard + (i < remainder ? 1 : 0);
      b.add(new Reader(seed + i, countHere));
    }
    return b.build();
  }

}
