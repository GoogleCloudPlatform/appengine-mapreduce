// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.common.collect.ImmutableList;

import junit.framework.TestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author ohler@google.com (Christian Ohler)
 */
public class MarshallersTest extends TestCase {

  private <T> void checkSimple(Marshaller<T> m, T value) throws IOException {
    ByteBuffer buf = m.toBytes(value);
    assertEquals(value, m.fromBytes(buf));
  }

  private <T> void checkByteOrder(Marshaller<T> m, T value) throws IOException {
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

  private <T> void checkMoreCapacity(Marshaller<T> m, T value) throws IOException {
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

  private <T> void checkTruncated(Marshaller<T> m, T value) throws IOException {
    ByteBuffer buf = m.toBytes(value);
    buf.limit(buf.limit() - 1);
    try {
      fail("Got " + m.fromBytes(buf));
    } catch (IOException e) {
      // ok
    }
  }

  private <T> void checkTrailingBytes(Marshaller<T> m, T value) throws IOException {
    ByteBuffer raw = m.toBytes(value);
    ByteBuffer wrapped = ByteBuffer.allocate(raw.remaining() + 1);
    wrapped.put(raw);
    wrapped.put((byte) 0);
    wrapped.flip();
    try {
      fail("Got " + m.fromBytes(wrapped));
    } catch (IOException e) {
      // ok
    }
  }

  private <T> void check(Marshaller<T> m, T value) throws IOException {
    checkSimple(m, value);
    checkByteOrder(m, value);
    checkMoreCapacity(m, value);
    checkTruncated(m, value);
    checkTrailingBytes(m, value);
  }

  public void testLongMarshaller() throws Exception {
    // The serialized format is unspecified, so we only check if the round-trip
    // works, not what the actual bytes are.
    Marshaller<Long> m = Marshallers.getLongMarshaller();
    for (long l = -1000; l < 1000; l++) {
      check(m, l);
    }
    for (long l = Long.MAX_VALUE - 1000; l != Long.MIN_VALUE + 1000; l++) {
      check(m, l);
    }
  }

  public void testIntegerMarshaller() throws Exception {
    // The serialized format is unspecified, so we only check if the round-trip
    // works, not what the actual bytes are.
    Marshaller<Integer> m = Marshallers.getIntegerMarshaller();
    for (int i = -1000; i < 1000; i++) {
      check(m, i);
    }
    for (int i = Integer.MAX_VALUE - 1000; i != Integer.MIN_VALUE + 1000; i++) {
      check(m, i);
    }
  }

  public void testStringMarshaller() throws Exception {
    // The serialized format is unspecified, so we only check if the round-trip
    // works, not what the actual bytes are.
    Marshaller<String> m = Marshallers.getStringMarshaller();
    for (String s : ImmutableList.of("", "a", "b", "ab", "foo",
            "\u0000", "\u0000\uabcd\u1234",
            "\ud801\udc02")) {
      check(m, s);
    }
  }

  public void testKeyValueMarshaller() throws Exception {
    // The serialized format is unspecified, so we only check if the round-trip
    // works, not what the actual bytes are.
    Marshaller<KeyValue<Long, String>> m = Marshallers.getKeyValueMarshaller(
        Marshallers.getLongMarshaller(),
        Marshallers.getStringMarshaller());
    for (KeyValue<Long, String> pair : ImmutableList.of(
            KeyValue.of(42L, "foo"),
            KeyValue.of(-1L, "bar"),
            KeyValue.of(Long.MAX_VALUE, ""),
            KeyValue.of(Long.MIN_VALUE, "iezvhofgfbbtgweggbhm"))) {
      check(m, pair);
    }
  }

}
