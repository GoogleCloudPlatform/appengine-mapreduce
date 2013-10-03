// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Turns objects of type {@code T} into bytes and back.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <T> type to be marshalled or unmarshalled
 */
public abstract class Marshaller<T> implements Serializable {
  private static final long serialVersionUID = 183874105234660517L;

  /**
   * Returns a new {@code ByteBuffer} {@code b} with a serialized representation
   * of {@code object} between {@code b.position()} and {@code b.limit()}.
   * {@code b.order()} is undefined.
   */
  public abstract ByteBuffer toBytes(T object);

  /**
   * Returns the object whose serialized representation is in {@code b} between
   * {@code b.position()} and {@code b.limit()}.  The value of {@code b.order()}
   * when the method is called is undefined, and this method may modify it as
   * well as {@code b.position()} and {@code b.limit()}.
   *
   * <p>The method may throw a {@link RuntimeException} if it determines that the
   * sequence of bytes in {@code b} was not generated by {@link #toBytes}.  This
   * includes corrupted data as well as trailing bytes.
   */
  public abstract T fromBytes(ByteBuffer b);

}
