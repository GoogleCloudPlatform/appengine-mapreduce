// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Produces all longs in a given interval.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public final class ConsecutiveLongInput extends Input<Long> {

  private static final long serialVersionUID = 722495043491410651L;

  @VisibleForTesting
  static class Reader extends InputReader<Long> {

    private static final long serialVersionUID = 796981411158026526L;

    private final long start;
    private final long limit;
    private long next;

    @VisibleForTesting
    Reader(long start, long limit) {
      this.start = start;
      this.limit = limit;
      next = start;
    }

    @Override
    public Double getProgress() {
      return limit <= start ? 1 : (next - start) / (double) (limit - start);
    }

    @Override
    public Long next() {
      if (next >= limit) {
        throw new NoSuchElementException();
      }
      return next++;
    }

    @Override
    public void beginShard() {
      next = start;
    }
  }

  private final int shardCount;
  private final long start;
  private final long limit;

  /**
   * Produces longs from {@long start} (inclusive) to {@code limit} (exclusive).
   * The interval is partitioned into {@code shardCount} subintervals of roughly
   * equal size, one for each shard.  Each shard produces its longs in ascending
   * order.
   */
  public ConsecutiveLongInput(long start, long limit, int shardCount) {
    Preconditions.checkArgument(shardCount > 0, "shardCount not positive: %s", shardCount);
    Preconditions.checkArgument(start <= limit, "start %s exceeds limit %s", start, limit);
    this.shardCount = shardCount;
    this.start = start;
    this.limit = limit;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + shardCount + ", " + start + ", " + limit + ")";
  }

  @Override
  public List<? extends InputReader<Long>> createReaders() {
    ImmutableList.Builder<InputReader<Long>> b = ImmutableList.builder();
    long valuesTotal = Math.max(0, limit - start);
    double valuesPerShard = valuesTotal / (double) shardCount;
    long shardStart = start;
    for (int i = 1; i <= shardCount; i++) {
      long shardLimit  = start + Math.round(i * valuesPerShard);
      b.add(new Reader(shardStart, shardLimit));
      shardStart = shardLimit;
    }
    assert shardStart == limit;
    return b.build();
  }
}
