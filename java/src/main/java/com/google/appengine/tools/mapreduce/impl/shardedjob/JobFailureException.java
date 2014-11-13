package com.google.appengine.tools.mapreduce.impl.shardedjob;

/**
 * An exception thrown when there should be no more attempts to continue processing the job.
 */
public class JobFailureException extends RuntimeException {

  private static final long serialVersionUID = -4481817785472768342L;

  public JobFailureException(String errorMessage) {
    super(errorMessage);
  }

  public JobFailureException(int shardNumber, Throwable rootCause) {
    super("Shard " + shardNumber + " failed the job", rootCause);
  }

  public JobFailureException(int shardNumber, String message, Throwable rootCause) {
    super("Shard " + shardNumber + " failed the job: " + message, rootCause);
  }
}
