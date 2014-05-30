// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;


/**
 * MapReduce context.
 */
public interface Context {

  /**
   * Returns the Id for the job.
   */
  String getJobId();
}
