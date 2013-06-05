// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Produces all longs in a given interval.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class ConsecutiveLongInput extends Input<Long> {
  private static final long serialVersionUID = 722495043491410651L;

  private static class Reader extends InputReader<Long> {
    private static final long serialVersionUID = 796981411158026526L;

    private final long start;
    private final long limit;
    private long next;

    private Reader(long start, long limit) {
      this.start = start;
      this.limit = limit;
      next = start;
    }

    @Override public Double getProgress() {
      return limit <= start ? 1
          : (next - start) / (double) (limit - start);
    }

    @Override public Long next() {
      if (next >= limit) {
        throw new NoSuchElementException();
      }
      return next++;
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

  @Override public String toString() {
    return getClass().getSimpleName() + "(" + shardCount + ", " + start + ", " + limit + ")";
  }

  @Override public List<? extends InputReader<Long>> createReaders() throws IOException {
    ImmutableList.Builder<InputReader<Long>> b = ImmutableList.builder();
    long valuesTotal = Math.max(0, limit - start);
    long valuesPerShard = valuesTotal / shardCount;
    long remainder = valuesTotal % shardCount;
    long nextStart = start;
    for (int i = 0; i < shardCount; i++) {
      long thisLimit = nextStart + valuesPerShard + (i < remainder ? 1 : 0);
      b.add(new Reader(nextStart, thisLimit));
      nextStart = thisLimit;
    }
    Preconditions.checkState(nextStart == limit, "%s != %s", nextStart, limit);
    return b.build();
  }

}
