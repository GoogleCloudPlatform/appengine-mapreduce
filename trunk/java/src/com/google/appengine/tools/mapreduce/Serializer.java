// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import java.nio.ByteBuffer;

/**
 * Encoder and Decoder for objects. The encode and decode functions should be
 * symmetric: For some T t, decode(encode(t)) == t.
 * @param <T> the type to be encoded.
 */
public interface Serializer<T> {

  /**
   * Encodes the input object into a {@link ByteBuffer}. The data representing the
   * object should sit in the {@link ByteBuffer#remaining()} portion of the
   * {@link ByteBuffer}.
   * @param type the input object
   * @return the {@link ByteBuffer} representing the object.
   * @throws {@link SerializerException} if it cannot encode the object into a
   * {@link ByteBuffer}.
   */
  public ByteBuffer encode(T type) throws SerializerException;


  /**
   * Decodes an object from a {@link ByteBuffer}. The data read from the
   * {@link ByteBuffer} should consist entirely of the {@link ByteBuffer#remaining()}
   * portion of the {@link ByteBuffer}.
   * @param buffer the {@link ByteBuffer} to read from.
   * @return the object decoded from the {@link ByteBuffer}.
   * @throws {@link SerializerException} if it cannot decode an object from the
   * {@link ByteBuffer}.
   */
  public T decode(ByteBuffer buffer) throws SerializerException;
}
