// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

/**
 * Built-in counter names.
 *
 */
public final class CounterNames {
  /**
   * Number of times map function was called.
   */
  public static final String MAPPER_CALLS = "mapper-calls";

  /**
   * Total time in milliseconds spent in map function.
   */
  public static final String MAPPER_WALLTIME_MILLIS = "mapper-walltime-msec";

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
