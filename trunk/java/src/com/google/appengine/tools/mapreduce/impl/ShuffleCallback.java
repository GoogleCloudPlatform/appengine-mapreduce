// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Settings for the task queue task used to notify the application when
 * shuffling is finished.
 * 
 */
public final class ShuffleCallback {
  private final String url;
  /*Nullable*/ private String appVersionId = null;
  /*Nullable*/ private String queue = null;
  // POST isn't that useful since query params don't work (task queue
  // restriction), and we can't specify a body.
  private String method = "GET";

  public ShuffleCallback(String url) {
    this.url = checkNotNull(url, "Null url");
  }

  public String getUrl() {
    return url;
  }

  /*Nullable*/ public String getAppVersionId() {
    return appVersionId;
  }

  public ShuffleCallback setAppVersionId(/*Nullable*/ String appVersionId) {
    this.appVersionId = appVersionId;
    return this;
  }

  /*Nullable*/ public String getQueue() {
    return queue;
  }

  public ShuffleCallback setQueue(/*Nullable*/ String queue) {
    this.queue = queue;
    return this;
  }

  public String getMethod() {
    return method;
  }

  public ShuffleCallback setMethod(String method) {
    this.method = checkNotNull(method, "Null method");
    return this;
  }

  @Override public String toString() {
    return getClass().getSimpleName() + "("
        + url + ", "
        + appVersionId + ", "
        + queue + ", "
        + method
        + ")";
  }
}