// Copyright 2014 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;

/**
 * An exception that is thrown upon MapReduceJob failure.
 */
public class MapReduceJobException extends RuntimeException {

  private static final long serialVersionUID = 2875093254119004898L;
  private final String stage;

  MapReduceJobException(String stage, Status status) {
    super("Stage " + stage + " was not completed successfuly (status="
        + status.getStatusCode() + ", message=" + status.getException().getMessage() + ")",
        status.getException());
    this.stage = stage;
  }

  /**
   * Returns a string representing the MapReduce stage that failed.
   * The exception propagated from the stage can be fetched by {@link #getCause()}.
   */
  public String getFailedStage() {
    return stage;
  }
}