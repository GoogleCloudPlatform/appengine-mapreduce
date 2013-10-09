// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.common.collect.ImmutableList;

import junit.framework.TestCase;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author ohler@google.com (Christian Ohler)
 */
public class MarshallersTest extends TestCase {

  private <T> void checkByteOrder(Marshaller<T> m, T value) {
    {
      ByteBuffer buf = m.toBytes(value);
      buf.order(ByteOrder.BIG_ENDIAN);
      assertEquals(value, m.fromBytes(buf));
    }
    {
      ByteBuffer buf2 = m.toBytes(value);
      buf2.order(ByteOrder.LITTLE_ENDIAN);
      assertEquals(value, m.fromBytes(buf2));
    }
  }

  private <T> void checkMoreCapacity(Marshaller<T> m, T value) {
    ByteBuffer raw = m.toBytes(value);
    ByteBuffer wrapped = ByteBuffer.allocate(raw.remaining() + 10);
    for (byte b = 0; b < 5; b++) {
      wrapped.put(b);
    }
    wrapped.put(raw);
    for (byte b = 0; b < 5; b++) {
      wrapped.put(b);
    }
    wrapped.position(5);
    wrapped.limit(wrapped.capacity() - 5);
    assertEquals(value, m.fromBytes(wrapped));
  }

  private <T> void checkTruncated(Marshaller<T> m, T value) {
    ByteBuffer buf = m.toBytes(value);
    buf.limit(buf.limit() - 1);
    try {
      fail("Got " + m.fromBytes(buf));
    } catch (RuntimeException e) {
      // ok
    }
  }

  private <T> void checkTrailingBytes(Marshaller<T> m, T value) {
    ByteBuffer raw = m.toBytes(value);
    ByteBuffer wrapped = ByteBuffer.allocate(raw.remaining() + 1);
    wrapped.put(raw);
    wrapped.put((byte) 0);
    wrapped.flip();
    try {
      fail("Got " + m.fromBytes(wrapped));
    } catch (RuntimeException e) {
      // ok
    }
  }

  private <T> void assertRoundTripEquality(Marshaller<T> marshaller, T value) {
    ByteBuffer bytes = marshaller.toBytes(value);
    T reconstructed = marshaller.fromBytes(bytes.slice());
    assertEquals(value, reconstructed);
    assertEquals(bytes, marshaller.toBytes(reconstructed));
  }

  /**
   * Perform checks that assume the data is not corrupted.
   */
  private <T> void preformValidChecks(Marshaller<T> m, T value) {
    assertRoundTripEquality(m, value);
    checkByteOrder(m, value);
    checkMoreCapacity(m, value);
  }

  /**
   * Perform all checks including those that attempt to detect corruption.
   */
  private <T> void preformAllChecks(Marshaller<T> m, T value) {
    preformValidChecks(m, value);
    checkTruncated(m, value);
    checkTrailingBytes(m, value);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testObjectMarshaller() {
    Marshaller m = Marshallers.getSerializationMarshaller();
    // Testing primitives
    preformAllChecks(m, 1);
    preformAllChecks(m, -1L);
    preformAllChecks(m, null);
    preformValidChecks(m,  "");
    preformValidChecks(m, "Foo");
    preformValidChecks(m, KeyValue.of(42L, "foo"));
    // Testing a large object
    Random r = new Random(0);
    preformAllChecks(m, new BigInteger(8 * (1024 * 1024 + 10), r));
    // Testing a map
    Map<String, String> map = new LinkedHashMap<String, String>();
    map.put("foo", "bar");
    map.put("baz", "bat");
    preformValidChecks(m, map);
    // Testing a nested object
    preformValidChecks(m, KeyValue.of(42L, KeyValue.of(42L, KeyValue.of(42L, "foo"))));
  }

  public void testLongMarshaller() throws Exception {
    // The serialized format is unspecified, so we only check if the round-trip
    // works, not what the actual bytes are.
    Marshaller<Long> m = Marshallers.getLongMarshaller();
    for (long l = -1000; l < 1000; l++) {
      preformAllChecks(m, l);
    }
    for (long l = Long.MAX_VALUE - 1000; l != Long.MIN_VALUE + 1000; l++) {
      preformAllChecks(m, l);
    }
  }

  public void testIntegerMarshaller() throws Exception {
    // The serialized format is unspecified, so we only check if the round-trip
    // works, not what the actual bytes are.
    Marshaller<Integer> m = Marshallers.getIntegerMarshaller();
    for (int i = -1000; i < 1000; i++) {
      preformAllChecks(m, i);
    }
    for (int i = Integer.MAX_VALUE - 1000; i != Integer.MIN_VALUE + 1000; i++) {
      preformAllChecks(m, i);
    }
  }

  public void testStringMarshaller() throws Exception {
    // The serialized format is unspecified, so we only check if the round-trip
    // works, not what the actual bytes are.
    Marshaller<String> m = Marshallers.getStringMarshaller();
    for (String s : ImmutableList.of("", "a", "b", "ab", "foo",
            "\u0000", "\u0000\uabcd\u1234",
            "\ud801\udc02")) {
      preformValidChecks(m, s);
    }
  }

  public void testKeyValueLongMarshaller() throws Exception {
    // The serialized format is unspecified, so we only check if the round-trip
    // works, not what the actual bytes are.
    Marshaller<KeyValue<Long, Long>> m = Marshallers.getKeyValueMarshaller(
        Marshallers.getLongMarshaller(), Marshallers.getLongMarshaller());
    for (KeyValue<Long, Long> pair :
        ImmutableList.of(KeyValue.of(42L, -1L), KeyValue.of(Long.MAX_VALUE, Long.MIN_VALUE))) {
      preformAllChecks(m, pair);
    }
  }

  public void testKeyValueStringMarshaller() throws Exception {
    // The serialized format is unspecified, so we only check if the round-trip
    // works, not what the actual bytes are.
    Marshaller<KeyValue<String, String>> m = Marshallers.getKeyValueMarshaller(
        Marshallers.getStringMarshaller(), Marshallers.getStringMarshaller());
    for (KeyValue<String, String> pair :
        ImmutableList.of(KeyValue.of("foo", "bar"), KeyValue.of("", "\u00a5123"))) {
      preformValidChecks(m, pair);
    }
  }

  public void testKeyValueIntegers() {
    Marshaller<Integer> intMarshaller = Marshallers.getIntegerMarshaller();
    Marshaller<KeyValue<Integer, Integer>> m =
        Marshallers.<Integer, Integer>getKeyValueMarshaller(intMarshaller, intMarshaller);
    for (int i = 0; i < 10000; i++) {
      assertRoundTripEquality(m, new KeyValue<Integer, Integer>(i, -i));
    }
  }

  public void testKeyValueNested() {
    Marshaller<Integer> intMarshaller = Marshallers.getIntegerMarshaller();
    Marshaller<String> stringMarshaller = Marshallers.getStringMarshaller();
    Marshaller<KeyValue<Integer, String>> nestedMarshaller =
        Marshallers.<Integer, String>getKeyValueMarshaller(intMarshaller, stringMarshaller);
    Marshaller<KeyValue<KeyValue<Integer, String>, KeyValue<Integer, String>>> m = Marshallers.<
        KeyValue<Integer, String>, KeyValue<Integer, String>>getKeyValueMarshaller(nestedMarshaller,
        nestedMarshaller);
    for (int i = 0; i < 10000; i++) {
      assertRoundTripEquality(m, new KeyValue<KeyValue<Integer, String>, KeyValue<Integer, String>>(
          new KeyValue<Integer, String>(i, "Foo" + i),
          new KeyValue<Integer, String>(-1, "Bar-" + i)));
    }
  }

}
