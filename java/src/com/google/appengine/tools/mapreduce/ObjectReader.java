// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.files.RecordReadChannel;

import java.io.IOException;

/**
 * Reads an object from a {@link RecordReadChannel}. This object is converted
 * using the supplied {@link Serializer}.
 *
 * @param <T> the object to read.
 */
public final class ObjectReader<T> {

  private final RecordReadChannel channel;
  private final Serializer<T> encoder;

  /**
   * Creates a new {@link ObjectReader}.
   *
   * @param encoder the {@link Serializer} for the type to read.
   * @param recordReadChannel the {@link RecordReadChannel} to read from.
   */
  public ObjectReader(Serializer<T> encoder, RecordReadChannel recordReadChannel) {
    this.encoder = encoder;
    this.channel = recordReadChannel;
  }

  /**
   * Reads an object from the {@link RecordReadChannel}.
   * @return the object.
   * @throws IOException if the {@link RecordReadChannel} has an exception, or the
   * {@link Serializer} cannot decode the object.
   */
  public T read() throws IOException {
    return encoder.decode(channel.readRecord());
  }
}
