// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

/**
 * Provides {@link ShardedJobService} implementations.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class ShardedJobServiceFactory {

  private ShardedJobServiceFactory() {}

  public static ShardedJobService getShardedJobService() {
    return new ShardedJobServiceImpl();
  }

}
