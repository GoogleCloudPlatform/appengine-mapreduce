// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

/**
 * <p>{@code Pool} provides a means of combining data from multiple invocations of {@code map()}
 * function call and for processing the combined data together.
 * A typical pool will store information in memory and flush when
 * needed or asked by framework. The most common type of pool is mutation pool, where various
 * mutation operations are batched together. </p>
 *
 * <p>Pools are obtained through {@link MapperContext} using the instance of PoolKey.
 * The instance is usually stored inside static variable for unique pool identification,
 * See {@link DatastoreMutationPool} for example.
 * </p>
 *
 * <p>This class is really an interface that might be evolving. In order to avoid breaking
 * users when we change the interface, we made it an abstract class.</p>
 *
 */
public abstract class Pool {

  /**
   * Flushes all outstanding mutations. Called by framework indicating that pool is about to be
   * destroyed.
   */
  public abstract void flush();

  /**
   * Pool key class.
   */
  public static class PoolKey<T extends Pool> {
    public final Class<? extends T> implClass;

    public PoolKey(Class<? extends T> implClass) {
      this.implClass = implClass;
    }
  }
}
