// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.files.FileServicePb.KeyValue;
import com.google.appengine.api.files.RecordReadChannel;

import java.io.IOException;

/**
 * Reads Key/Value pairs from a RecordReadChannel.
 *
 *
 * @param <K> the key type.
 * @param <V> the value type.
 */
public final class KeyValueReader<K, V> {

  private final ObjectReader<KeyValue> reader;
  private final Serializer<K> keyEncoder;
  private final Serializer<V> valueEncoder;

  /**
   * @param <K> the key type.
   * @param <V> the value type.
   */
  public static class KeyValuePair<K, V> {
    private final K key;
    private final V value;

    private KeyValuePair(K key, V value) {
      this.key = key;
      this.value = value;
    }
    public K key() {
      return key;
    }
    public V value() {
      return value;
    }
  }

  /**
   * Creates a new {@link KeyValueReader}
   * @param keyEncoder the {@link Serializer} for the Key type.
   * @param valueEncoder the {@link Serializer} for the Value type.
   * @param channel the {@link RecordReadChannel} to write to.
   */
  public KeyValueReader(Serializer<K> keyEncoder,
                        Serializer<V> valueEncoder,
                        RecordReadChannel channel) {
    reader = new ObjectReader<KeyValue>(new Serializers.KeyValueEncoder(), channel);
    this.keyEncoder = keyEncoder;
    this.valueEncoder = valueEncoder;
  }

  /**
   * Reads a {@link KeyValuePair} from the {@link RecordReadChannel}
   * @return the {@link KeyValuePair} read.
   * @throws IOException thrown if the underlying {@link RecordReadChannel} throws an exception.
   */
  public KeyValuePair<K, V> read() throws IOException {
    KeyValue kv = reader.read();
    return new KeyValuePair<K, V>(keyEncoder.decode(kv.getKeyBytes().asReadOnlyByteBuffer()),
                            valueEncoder.decode(kv.getValueBytes().asReadOnlyByteBuffer()));
  }
}
