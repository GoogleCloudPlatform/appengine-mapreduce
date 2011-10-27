// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.files.RecordWriteChannel;

import java.io.IOException;

/**
 * Write an object to a {@link RecordWriteChannel}. This object is converted
 * using the supplied {@link Serializer}.
 *
 * @param <T> the type to write.
 */
public final class ObjectWriter<T> {

  private final RecordWriteChannel output;
  private final Serializer<T> encoder;

  /**
   * Creates a new {@link ObjectWriter}.
   *
   * @param encoder the {@link Serializer} for the type to write.
   * @param recordWriteChannel the {@link RecordWriteChannel} to write to.
   */
  public ObjectWriter(Serializer<T> encoder, RecordWriteChannel recordWriteChannel) {
    this.encoder = encoder;
    this.output = recordWriteChannel;
  }

  /**
   * Writes an object to the {@link RecordWriteChannel}.
   * @param object the object to write.
   * @throws IOException if the {@link RecordWriteChannel} has an exception, or
   * the {@link Serializer} cannot encode the object.
   */
  public void write(T object) throws IOException {
    output.write(encoder.encode(object));
  }

  /**
   * Writes an object to the {@link RecordWriteChannel} using a sequence key.
   * @param object the object to write.
   * @param sequenceKey see {@link RecordWriteChannel#write(java.nio.ByteBuffer, String)}
   * @throws IOException if the {@link RecordWriteChannel} has an exception, or
   * the {@link Serializer} cannot encode the object.
   */
  public void write(T object, String sequenceKey) throws IOException {
    output.write(encoder.encode(object), sequenceKey);
  }

}
