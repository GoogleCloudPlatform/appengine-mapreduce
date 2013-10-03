package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.CorruptDataException;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;

import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Tests for {@link KeyValueMarshaller} This asserts that both the item can be read back once
 * written and that the read back item if written again will produce an identical byte array. This
 * means the serialization cannot depend on identity in anyway.
 *
 */
public class KeyValueMarshallerTest extends TestCase {

  private <K, V> void assertRoundTripEquality(KeyValueMarshaller<K, V> marshaller,
      KeyValue<K, V> keyValue) {
    ByteBuffer bytes = marshaller.toBytes(keyValue);
    KeyValue<K, V> reconstructed = marshaller.fromBytes(bytes.slice());
    assertEquals(keyValue, reconstructed);
    assertEquals(bytes, marshaller.toBytes(reconstructed));
  }

  public void testThrowsCorruptDataException() {
    Marshaller<ByteBuffer> byteBufferMarshaller = Marshallers.getByteBufferMarshaller();
    KeyValueMarshaller<ByteBuffer, ByteBuffer> m =
        new KeyValueMarshaller<ByteBuffer, ByteBuffer>(byteBufferMarshaller, byteBufferMarshaller);
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

  public void testRandomData() {
    Marshaller<ByteBuffer> byteBufferMarshaller = Marshallers.getByteBufferMarshaller();
    KeyValueMarshaller<ByteBuffer, ByteBuffer> m =
        new KeyValueMarshaller<ByteBuffer, ByteBuffer>(byteBufferMarshaller, byteBufferMarshaller);
    Random r = new Random(0);
    for (int i = 0; i < 10000; i++) {
      ByteBuffer key = getRandomByteBuffer(r);
      ByteBuffer value = getRandomByteBuffer(r);
      assertRoundTripEquality(m, new KeyValue<ByteBuffer, ByteBuffer>(key, value));
    }
  }



  private ByteBuffer getRandomByteBuffer(Random r) {
    byte[] bytes = new byte[10];
    r.nextBytes(bytes);
    ByteBuffer b = ByteBuffer.wrap(bytes);
    return b;
  }

}
