// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.appengine.tools.mapreduce.Serializers.strings;

import junit.framework.TestCase;

import java.io.IOException;

/**
 *
 */
public class KeyValueReaderWriterTest extends TestCase {

  /**
   * Helper class to manage KeyValue Reader and Writer for tests.
   */
  private static final class Tester<K, V> {
    private MockRecordChannel channel;
    private KeyValueWriter<K, V> writer;
    private KeyValueReader<K, V> reader;

    public Tester(Serializer<K> keyEncoder, Serializer<V> valueEncoder) {
      channel = new MockRecordChannel();
      writer = new KeyValueWriter<K, V>(keyEncoder, valueEncoder, channel);
      reader = new KeyValueReader<K, V>(keyEncoder, valueEncoder, channel);
    }

    public void write(K key, V value) throws IOException {
      writer.write(key, value);
    }

    public KeyValueReader.KeyValuePair<K, V> read() throws IOException {
      return reader.read();
    }
  }

  /**
   * Tests writing then reading a String,String KeyValue.
   */
  public void testStringReadWrite() throws Exception {
    Tester<String, String> t = new Tester<String, String>(strings(), strings());
    t.write("key", "value");
    KeyValueReader.KeyValuePair<String, String> result = t.read();
    assertEquals("key", result.key());
    assertEquals("value", result.value());
  }

  /**
   * Tests writing then reading a String/String KeyValue using a sequence key.
   */
  public void testStringWriteWithSequenceKey() throws Exception {
    Tester<String, String> t = new Tester<String, String>(strings(), strings());
    t.writer.write("key", "value", "1234");
    KeyValueReader.KeyValuePair<String, String> result = t.read();
    assertEquals("key", result.key());
    assertEquals("value", result.value());
  }
}
