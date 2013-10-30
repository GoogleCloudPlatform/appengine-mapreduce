package com.google.appengine.tools.mapreduce.impl.shardedjob;

/**
 * An exception thrown when there should be no more attempts to continue processing the shard.
 */
public class ShardFailureException extends RuntimeException {

  private static final long serialVersionUID = -1082842736486563617L;

  public ShardFailureException(int shardNumber, Throwable rootCause) {
    super("Shard " + shardNumber + " failed.", rootCause);
  }
}
