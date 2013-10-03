// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import java.io.Serializable;

/**
 * Collection of all counters.
 *
 */
public interface Counters extends Serializable {

  /**
   * @param name counter name
   * @return counter with a given name. Creates new counter with 0 value if it doesn't exist.
   */
  Counter getCounter(String name);

  /**
   * @return iterable over all created counters.
   */
  Iterable<? extends Counter> getCounters();

  /**
   * @param other Another counter object who's counters should all be added to this one.
   */
  public void addAll(Counters other);
}
