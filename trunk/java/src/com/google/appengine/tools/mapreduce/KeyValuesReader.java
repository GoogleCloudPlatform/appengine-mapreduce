// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.files.FileServicePb.KeyValues;
import com.google.appengine.api.files.RecordReadChannel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads Key - List of Values pairs from a RecordReadChannel.
 *
 *
 * @param <K> the key type.
 * @param <V> the value type.
 */
public final class KeyValuesReader<K, V> {

  private final ObjectReader<KeyValues> reader;
  private final Serializer<K> keyEncoder;
  private final Serializer<V> valueEncoder;

  /**
   * @param <K> the key type.
   * @param <V> the value type.
   */
  public static class KeyValuesPair<K, V> {
    private final K key;
    private final List<V> values;

    private KeyValuesPair(K key, List<V> values) {
      this.key = key;
      this.values = values;
    }
    public K key() {
      return key;
    }
    public List<V> values() {
      return values;
    }
  }

  /**
   * Creates a new {@link KeyValueReader}
   * @param keyEncoder the {@link Serializer} for the Key type.
   * @param valueEncoder the {@link Serializer} for the Value types.
   * @param channel the {@link RecordReadChannel} to write to.
   */
  public KeyValuesReader(Serializer<K> keyEncoder,
                        Serializer<V> valueEncoder,
                        RecordReadChannel channel) {
    reader = new ObjectReader<KeyValues>(new Serializers.KeyValuesEncoder(), channel);
    this.keyEncoder = keyEncoder;
    this.valueEncoder = valueEncoder;
  }

  /**
   * Reads a {@link KeyValuesPair} from the {@link RecordReadChannel}
   * @return the {@link KeyValuesPair} read.
   * @throws IOException thrown if the underlying {@link RecordReadChannel} throws an exception.
   */
  public KeyValuesPair<K, V> read() throws IOException {
    KeyValues kv = reader.read();
    List<V> values = new ArrayList<V>();
    for (int i = 0; i < kv.getValueCount(); i++) {
      values.add(valueEncoder.decode(kv.getValueBytes(i).asReadOnlyByteBuffer()));
    }
    return new KeyValuesPair<K, V>(keyEncoder.decode(kv.getKeyBytes().asReadOnlyByteBuffer()),
                                   values);
  }
}
