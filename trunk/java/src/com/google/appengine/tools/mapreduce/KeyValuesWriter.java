// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.files.FileServicePb.KeyValues;
import com.google.appengine.api.files.RecordWriteChannel;
import com.google.appengine.repackaged.com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.List;

/**
 * Writes Key/Value pairs to a RecordWriteChannel.
 *
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class KeyValuesWriter <K, V>{

  private final ObjectWriter<KeyValues> writer;
  private final Serializer<K> keyEncoder;
  private final Serializer<V> valueEncoder;


  public KeyValuesWriter(Serializer<K> keyEncoder,
                        Serializer<V> valueEncoder,
                        RecordWriteChannel channel) {
    this.keyEncoder = keyEncoder;
    this.valueEncoder = valueEncoder;
    this.writer = new ObjectWriter<KeyValues>(new Serializers.KeyValuesEncoder(), channel);
  }

  /**
   * Writes a key/value pair to a {@link RecordWriteChannel} with a sequenceKey
   * to help avoid rewriting data.
   * @param key the key to write.
   * @param values the list of values to write.
   * @param sequenceKey a sequence key, see
   * {@link com.google.appengine.api.files.FileWriteChannel#write(java.nio.ByteBuffer, java.lang.String)}
   * @throws IOException if the underlying channel throws an exception.
   */
  public void write(K key, List<V> values, String sequenceKey) throws IOException {
    KeyValues.Builder builder = KeyValues.newBuilder();
    builder.setKeyBytes(ByteString.copyFrom(keyEncoder.encode(key)));
    for (V value : values) {
      builder.addValueBytes(ByteString.copyFrom(valueEncoder.encode(value)));
    }
    KeyValues kv = builder.build();
    if (sequenceKey != null) {
      writer.write(kv, sequenceKey);
    } else {
      writer.write(kv);
    }
  }

  /**
   * Writes a key/value pair to a {@link RecordWriteChannel}
   * @param key the key to write.
   * @param values the list of values to write.
   * @throws IOException if the underlying channel throws an exception.
   */
  public void write(K key, List<V> values) throws IOException{
    write(key, values, null);
  }
}
