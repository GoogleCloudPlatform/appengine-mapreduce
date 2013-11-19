// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

/**
 * Built-in counter names.
 *
 */
public final class CounterNames {

  // TODO(user): There is a fundamental flaw with all of this counters.
  // The counters only represent the aggregates of the last successful shard
  // and ignore the calls/cost of slice retries. In order to change that
  // we would need to move the notion of the counters to sharded-job or
  // to persist counters separately from partialResult (R).

  /**
   * Number of times map function was called.
   */
  public static final String MAPPER_CALLS = "mapper-calls";

  /**
   * Total time in milliseconds spent in map function.
   */
  public static final String MAPPER_WALLTIME_MILLIS = "mapper-walltime-msec";

  /**
   * Number of times sort function was called.
   */
  public static final String SORT_CALLS = "sort-calls";

  /**
   * Total time in milliseconds spent in sort function.
   */
  public static final String SORT_WALLTIME_MILLIS = "sort-walltime-msec";

  /**
   * Number of times reduce function was called.
   */
  public static final String REDUCER_CALLS = "reducer-calls";

  /**
   * Total time in milliseconds spent in reduce function.
   */
  public static final String REDUCER_WALLTIME_MILLIS = "reducer-walltime-msec";

  /**
   * Total number of bytes written to the output.
   */
  public static final String IO_WRITE_BYTES = "io-write-bytes";

  /**
   * Total time in milliseconds spent writing.
   */
  public static final String IO_WRITE_MILLIS = "io-write-msec";

  /**
   * Total number of bytes read.
   */
  public static final String IO_READ_BYTES = "io-read-bytes";

  /**
   * Total time in milliseconds spent reading.
   */
  public static final String IO_READ_MILLIS = "io-read-msec";

  private CounterNames() {}
}
