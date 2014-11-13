package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.Context;


/**
 * Base class for all Context implementations.
 */
public class BaseContext implements Context {

  private final String jobId;

  public BaseContext(String jobId) {
    this.jobId = jobId;
  }

  @Override
  public String getJobId() {
    return jobId;
  }
}