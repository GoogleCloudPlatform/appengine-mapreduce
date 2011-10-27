// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Tests the provided {@link Serializer} objects for primitive types.
 *
 */
public class SerializerTest extends TestCase {

  public void testString() throws Exception {
    testEncoder(Serializers.strings(), "Hello world!");
  }

  public void testBoolean() throws Exception {
    testEncoder(Serializers.bools(), true);
    testEncoder(Serializers.bools(), false);
  }

  public void testByte() throws Exception {
    testEncoder(Serializers.bytes(), (byte) 0x90);
  }

  public void testShort() throws Exception {
    testEncoder(Serializers.shorts(), (short) 55);
  }

  public void testChar() throws Exception {
    testEncoder(Serializers.chars(), 'a');
  }

  public void testInt() throws Exception {
    testEncoder(Serializers.ints(), 443565);
  }

  public void testFloat() throws Exception {
    testEncoder(Serializers.floats(), (float) 453.34);
  }

  public void testLong() throws Exception {
    testEncoder(Serializers.longs(), 2147483648L);
  }

  public void testDouble() throws Exception {
    testEncoder(Serializers.doubles(), 2147483648.33);
  }

  public void testByteArray() throws Exception {
    Serializer<byte[]> encoder = Serializers.byteArray();
    byte[] value = new byte[] { 1, 2 , 3, 4};
    ByteBuffer bb = encoder.encode(value);
    assertArrayEquals(value, encoder.decode(bb));
  }

  /**
   * Our encoders should throw a {@link NullPointerException} if they are
   * passed a null pointer.
   */
  public void testNull() throws Exception {
    try {
      testEncoder(Serializers.strings(), null);
      fail("Test encoder should throw NullPointerException on receiving null pointer.");
    } catch (NullPointerException expected) {
    }
    try {
      testEncoder(Serializers.ints(), null);
      fail("Test encoder should throw NullPointerException on receiving null pointer.");
    } catch (NullPointerException expected) {
    }
  }

  public void testInvalidBooleanLength() throws Exception {
    testEncoderWithBadLength(Serializers.bools(), true);
    testEncoderWithBadLength(Serializers.bools(), false);
  }

  public void testInvalidByteLength() throws Exception {
    testEncoderWithBadLength(Serializers.bytes(), (byte) 0x90);
  }

  public void testInvalidShortLength() throws Exception {
    testEncoderWithBadLength(Serializers.shorts(), (short) 55);
  }

  public void testInvalidCharLength() throws Exception {
    testEncoderWithBadLength(Serializers.chars(), 'a');
  }

  public void testInvalidIntegerLength() throws Exception {
    testEncoderWithBadLength(Serializers.ints(), 443565);
  }

  public void testInvalidFloatLength() throws Exception {
    testEncoderWithBadLength(Serializers.floats(), (float) 453.34);
  }

  public void testInvalidLongLength() throws Exception {
    testEncoderWithBadLength(Serializers.longs(), 2147483648L);
  }

  public void testInvalidDoubleLength() throws Exception {
    testEncoderWithBadLength(Serializers.doubles(), 2147483648.33);
  }

  private <T> void testEncoderWithBadLength(Serializer<T> encoder, T value) throws Exception {
    ByteBuffer bb = encoder.encode(value);
    ByteBuffer invalidBB = ByteBuffer.wrap(Arrays.copyOf(bb.array(), bb.capacity() + 1));
    try {
      encoder.decode(invalidBB);
      fail();
    } catch (SerializerException expected) {}
  }



  private <T> void testEncoder(Serializer<T> encoder, T value) throws Exception {
    ByteBuffer bb = encoder.encode(value);
    assertEquals(value, encoder.decode(bb));
  }

  /*
   * Private method to test that two arrays are equal.
   */
  private void assertArrayEquals(byte[] expected, byte[] actual) {
    if (expected.length != actual.length){
      fail("Array lengths don't match. Expected:" +
          expected.length + " but was:" + actual.length);
    }
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] != actual[i]) {
        String expectedStr = Integer.toHexString(expected[i] & 0xFF);
        String actualStr = Integer.toHexString(actual[i] & 0xFF);
        fail("Expected:<" + expectedStr +
            "> but was:<" + actualStr + "> at index:" + i);
      }
    }
  }
}
