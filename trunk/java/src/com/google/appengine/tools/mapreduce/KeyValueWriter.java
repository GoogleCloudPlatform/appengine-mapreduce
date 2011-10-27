// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.files.FileServicePb.KeyValue;
import com.google.appengine.api.files.RecordWriteChannel;
import com.google.appengine.repackaged.com.google.protobuf.ByteString;

import java.io.IOException;

/**
 * Writes Key/Value pairs to a RecordWriteChannel.
 *
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class KeyValueWriter <K, V>{

  private final ObjectWriter<KeyValue> writer;
  private final Serializer<K> keyEncoder;
  private final Serializer<V> valueEncoder;


  public KeyValueWriter(Serializer<K> keyEncoder,
                        Serializer<V> valueEncoder,
                        RecordWriteChannel channel) {
    this.keyEncoder = keyEncoder;
    this.valueEncoder = valueEncoder;
    this.writer = new ObjectWriter<KeyValue>(new Serializers.KeyValueEncoder(), channel);
  }

  /**
   * Writes a key/value pair to a {@link RecordWriteChannel} with a sequenceKey
   * to help avoid rewriting data.
   * @param key the key to write.
   * @param value the value to write.
   * @param sequenceKey a sequence key, see
   * {@link com.google.appengine.api.files.FileWriteChannel#write(java.nio.ByteBuffer, java.lang.String)}
   * @throws IOException if the underlying channel throws an exception.
   */
  public void write(K key, V value, String sequenceKey) throws IOException {
    KeyValue kv = KeyValue.newBuilder().setKeyBytes(ByteString.copyFrom(keyEncoder.encode(key)))
        .setValueBytes(ByteString.copyFrom(valueEncoder.encode(value)))
        .build();
    if (sequenceKey != null) {
      writer.write(kv, sequenceKey);
    } else {
      writer.write(kv);
    }
  }

  /**
   * Writes a key/value pair to a {@link RecordWriteChannel}
   * @param key the key to write.
   * @param value the value to write.
   * @throws IOException if the underlying channel throws an exception.
   */
  public void write(K key, V value) throws IOException{
    write(key, value, null);
  }
}
