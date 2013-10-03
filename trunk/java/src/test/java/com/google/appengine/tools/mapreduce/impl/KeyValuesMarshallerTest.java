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
    ByteBuffer bytes = marshaller.toBytes(new KeyValue<K, Iterator<V>>(key, values.iterator()));
    KeyValue<K, Iterator<V>> reconstructed = marshaller.fromBytes(bytes.slice());
    validateEqual(key, values, reconstructed);

    reconstructed = marshaller.fromBytes(bytes.slice());
    assertEquals(bytes, marshaller.toBytes(reconstructed));
  }

  private <K, V> void validateEqual(K key, List<V> values, KeyValue<K, Iterator<V>> reconstructed) {
    assertEquals(key, reconstructed.getKey());
    Iterator<V> reconValues = reconstructed.getValue();
    for (V value : values) {
      assertEquals(value, reconValues.next());
    }
    assertFalse(reconValues.hasNext());
  }

  public void testRandomData() {
    Marshaller<ByteBuffer> byteBufferMarshaller = Marshallers.getByteBufferMarshaller();
    KeyValuesMarshaller<ByteBuffer, ByteBuffer> m =
        new KeyValuesMarshaller<ByteBuffer, ByteBuffer>(byteBufferMarshaller, byteBufferMarshaller);
    Random r = new Random(0);
    for (int i = 0; i < 1000; i++) {
      ByteBuffer key = getRandomByteBuffer(r);
      ArrayList<ByteBuffer> values = new ArrayList<ByteBuffer>();
      for (int j = 0; j < 10; j++) {
        values.add(getRandomByteBuffer(r));
      }
      assertRoundTripEquality(m, key, values);
    }
  }

  public void testNoValues() {
    Marshaller<String> stringMarshaller = Marshallers.getStringMarshaller();
    KeyValuesMarshaller<String, String> m =
        new KeyValuesMarshaller<String, String>(stringMarshaller, stringMarshaller);
    String key = "Foo";
    assertRoundTripEquality(m, key, new ArrayList<String>(0));
  }

  public void testThrowsCorruptDataException() {
    Marshaller<ByteBuffer> byteBufferMarshaller = Marshallers.getByteBufferMarshaller();
    KeyValuesMarshaller<ByteBuffer, ByteBuffer> m =
        new KeyValuesMarshaller<ByteBuffer, ByteBuffer>(byteBufferMarshaller, byteBufferMarshaller);
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
        new KeyValuesMarshaller<Integer, String>(intMarshaller, stringMarshaller);
    KeyValuesMarshaller<KeyValue<Integer, Iterator<String>>, KeyValue<Integer, Iterator<String>>>
        m = new KeyValuesMarshaller<
        KeyValue<Integer, Iterator<String>>, KeyValue<Integer, Iterator<String>>>(
        nestedMarshaller, nestedMarshaller);

    ArrayList<String> keyStrings = new ArrayList<String>();
    ArrayList<String> valueStrings = new ArrayList<String>();
    for (int k = 0; k < 10; k++) {
      keyStrings.add("KeyString-" + k);
      valueStrings.add("ValueString-" + k);
    }

    for (int i = 0; i < 10; i++) {
      KeyValue<Integer, Iterator<String>> key =
          new KeyValue<Integer, Iterator<String>>(i, keyStrings.iterator());
      List<KeyValue<Integer, Iterator<String>>> valueValues =
          new ArrayList<KeyValue<Integer, Iterator<String>>>();
      for (int j = 0; j < 10000; j++) {
        valueValues.add(new KeyValue<Integer, Iterator<String>>(-i, valueStrings.iterator()));
      }

      ByteBuffer bytes = m.toBytes(new KeyValue<KeyValue<Integer, Iterator<String>>,
          Iterator<KeyValue<Integer, Iterator<String>>>>(key, valueValues.iterator()));
      KeyValue<KeyValue<Integer, Iterator<String>>, Iterator<KeyValue<Integer, Iterator<String>>>>
          reconstructed = m.fromBytes(bytes.slice());
      validateEqual(key.getKey(), keyStrings, reconstructed.getKey());

      Iterator<KeyValue<Integer, Iterator<String>>> reconValues = reconstructed.getValue();
      for (KeyValue<Integer, Iterator<String>> value : valueValues) {
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
