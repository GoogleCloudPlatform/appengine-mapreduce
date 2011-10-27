// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.appengine.tools.mapreduce.Serializers.strings;

import com.google.appengine.tools.mapreduce.KeyValuesReader.KeyValuesPair;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class KeyValuesReaderWriterTest extends TestCase {

  private static final class Tester<K, V> {
    private MockRecordChannel channel;
    private KeyValuesWriter<K, V> writer;
    private KeyValuesReader<K, V> reader;

    public Tester(Serializer<K> keyEncoder, Serializer<V> valueEncoder) {
      channel = new MockRecordChannel();
      writer = new KeyValuesWriter<K, V>(keyEncoder, valueEncoder, channel);
      reader = new KeyValuesReader<K, V>(keyEncoder, valueEncoder, channel);
    }

    public void write(K key, List<V> values) throws IOException {
      writer.write(key, values);
    }

    public KeyValuesPair<K, V> read() throws IOException {
      return reader.read();
    }
  }

  /**
   * Tests a basic write.
   * @throws Exception
   */
  public void testStringReadWrite() throws Exception {
    Tester<String, String> t = new Tester<String, String>(strings(), strings());
    t.write("key", Arrays.asList("value1", "value2"));
    KeyValuesPair<String, String> result = t.read();
    assertEquals("key", result.key());
    assertListEquals(Arrays.asList("value1", "value2"), result.values());
  }

  /**
   * Tests writing using a sequence key.
   * @throws Exception
   */
  public void testWriteWithSequenceKey() throws Exception {
    Tester<String, String> t = new Tester<String, String>(strings(), strings());
    t.writer.write("key", Arrays.asList("value1", "value2"), "1234");
    KeyValuesPair<String, String> result = t.read();
    assertEquals("key", result.key());
    assertListEquals(Arrays.asList("value1", "value2"), result.values());
  }

  /**
   * Asserts that the contents of two lists are equal.
   * @param expected the expected list.
   * @param actual the actual list.
   * @throws Exception if the contents are not equal.
   */
  private <T> void assertListEquals(List<T> expected, List<T> actual) throws Exception {
    if (expected.size() != actual.size()) {
      throw new AssertionError("Expected list of size " + expected.size()
                               + " but got list of size " + actual.size() + ".");
    }
    for (int i = 0; i < expected.size(); i++) {
      if (!expected.get(i).equals(actual.get(i))) {
        throw new AssertionError("Expected " + expected.get(i) + " at position "
                                 + i + " but got " + actual.get(i));
      }
    }
  }
}
