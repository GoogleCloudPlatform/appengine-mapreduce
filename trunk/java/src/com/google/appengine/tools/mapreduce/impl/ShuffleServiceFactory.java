// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;


/**
 * A factory for producing instances of {@link ShuffleService}.
 */
public class ShuffleServiceFactory {
  /**
   * Returns an instance of {@link ShuffleService}.
   */
  public static ShuffleService getShuffleService() {
    return new ShuffleServiceImpl();
  }
}
