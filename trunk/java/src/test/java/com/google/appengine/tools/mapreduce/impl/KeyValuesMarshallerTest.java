package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.CorruptDataException;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;

import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Tests for {@link KeyValuesMarshaller}. This asserts that both the item can be read back once
 * written and that the read back item if written again will produce an identical byte array. This
 * means the serialization cannot depend on identity in anyway.
 *
 */
public class KeyValuesMarshallerTest extends TestCase {

  private <K, V> void assertRoundTripEquality(KeyValuesMarshaller<K, V> marshaller, K key,
      List<V> values) {
    ByteBuffer bytes = marshaller.toBytes(new KeyValue<>(key, values));
    KeyValue<K, Iterable<V>> reconstructed = marshaller.fromBytes(bytes.slice());
    validateEqual(key, values, reconstructed);
    reconstructed = marshaller.fromBytes(bytes.slice());
    assertEquals(bytes, marshaller.toBytes(reconstructed));
  }

  private <K, V> void validateEqual(K key, List<V> values, KeyValue<K, ? extends Iterable<V>> reconstructed) {
    assertEquals(key, reconstructed.getKey());
    Iterator<V> reconValues = reconstructed.getValue().iterator();
    for (V value : values) {
      assertEquals(value, reconValues.next());
    }
    assertFalse(reconValues.hasNext());
  }

  public void testRandomData() {
    Marshaller<ByteBuffer> byteBufferMarshaller = Marshallers.getByteBufferMarshaller();
    KeyValuesMarshaller<ByteBuffer, ByteBuffer> m =
        new KeyValuesMarshaller<>(byteBufferMarshaller, byteBufferMarshaller);
    Random r = new Random(0);
    for (int i = 0; i < 1000; i++) {
      ByteBuffer key = getRandomByteBuffer(r);
      ArrayList<ByteBuffer> values = new ArrayList<>();
      for (int j = 0; j < 10; j++) {
        values.add(getRandomByteBuffer(r));
      }
      assertRoundTripEquality(m, key, values);
    }
  }

  public void testNoValues() {
    Marshaller<String> stringMarshaller = Marshallers.getStringMarshaller();
    KeyValuesMarshaller<String, String> m =
        new KeyValuesMarshaller<>(stringMarshaller, stringMarshaller);
    String key = "Foo";
    assertRoundTripEquality(m, key, new ArrayList<String>(0));
  }

  public void testThrowsCorruptDataException() {
    Marshaller<ByteBuffer> byteBufferMarshaller = Marshallers.getByteBufferMarshaller();
    KeyValuesMarshaller<ByteBuffer, ByteBuffer> m =
        new KeyValuesMarshaller<>(byteBufferMarshaller, byteBufferMarshaller);
    Random r = new Random(0);
    ByteBuffer key = getRandomByteBuffer(r);
    try {
      m.fromBytes(key);
      fail();
    } catch (CorruptDataException e) {
      // Expected
    }
    try {
      m.fromBytes(ByteBuffer.allocate(0));
      fail();
    } catch (CorruptDataException e) {
      // Expected
    }
  }

  public void testNested() {
    Marshaller<Integer> intMarshaller = Marshallers.getIntegerMarshaller();
    Marshaller<String> stringMarshaller = Marshallers.getStringMarshaller();
    KeyValuesMarshaller<Integer, String> nestedMarshaller =
        new KeyValuesMarshaller<>(intMarshaller, stringMarshaller);
    KeyValuesMarshaller<KeyValue<Integer, ? extends Iterable<String>>,
        KeyValue<Integer, ? extends Iterable<String>>> m =
            new KeyValuesMarshaller<>(nestedMarshaller, nestedMarshaller);

    List<String> keyStrings = new ArrayList<>();
    List<String> valueStrings = new ArrayList<>();
    for (int k = 0; k < 10; k++) {
      keyStrings.add("KeyString-" + k);
      valueStrings.add("ValueString-" + k);
    }

    for (int i = 0; i < 10; i++) {
      KeyValue<Integer, List<String>> key = new KeyValue<>(i, keyStrings);
      List<KeyValue<Integer, List<String>>> valueValues = new ArrayList<>();
      for (int j = 0; j < 10000; j++) {
        valueValues.add(new KeyValue<>(-i, valueStrings));
      }

      @SuppressWarnings({"unchecked", "rawtypes"})
      ByteBuffer bytes = m.toBytes(new KeyValue(key, valueValues));
      KeyValue<KeyValue<Integer, ? extends Iterable<String>>,
          Iterable<KeyValue<Integer, ? extends Iterable<String>>>> reconstructed =
              m.fromBytes(bytes.slice());
      validateEqual(key.getKey(), keyStrings, reconstructed.getKey());

      Iterator<KeyValue<Integer, ? extends Iterable<String>>> reconValues =
          reconstructed.getValue().iterator();
      for (KeyValue<Integer, ? extends List<String>> value : valueValues) {
        validateEqual(value.getKey(), valueStrings, reconValues.next());
      }
      assertFalse(reconValues.hasNext());
    }
  }

  private ByteBuffer getRandomByteBuffer(Random r) {
    byte[] bytes = new byte[10];
    r.nextBytes(bytes);
    ByteBuffer b = ByteBuffer.wrap(bytes);
    return b;
  }
}
