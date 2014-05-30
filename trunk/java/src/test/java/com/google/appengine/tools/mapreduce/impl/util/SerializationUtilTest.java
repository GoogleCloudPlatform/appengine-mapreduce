// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.util;

import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil.CompressionType;

import junit.framework.TestCase;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author ohler@google.com (Christian Ohler)
 */
public class SerializationUtilTest extends TestCase {

  public void testGetBytes_slice1() throws Exception {
    ByteBuffer b = ByteBuffer.allocate(10);
    b.putShort((short) 0x1234);
    b.limit(2);
    b.position(0);
    ByteBuffer slice = b.slice();
    byte[] bytes = SerializationUtil.getBytes(slice);
    assertEquals(2, bytes.length);
    assertTrue(Arrays.equals(new byte[] { 0x12, 0x34 }, bytes));
  }

  public void testGetBytes_slice2() throws Exception {
    ByteBuffer b = ByteBuffer.allocate(10);
    b.position(2);
    b.putShort((short) 0x1234);
    b.position(2);
    b.limit(4);
    ByteBuffer slice = b.slice();
    byte[] bytes = SerializationUtil.getBytes(slice);
    assertEquals(2, bytes.length);
    assertTrue(Arrays.equals(new byte[] { 0x12, 0x34 }, bytes));
  }

  public void testSerializeToFromByteArrayWithNoParams() throws Exception {
    Serializable original = "hello";
    byte[] bytes = SerializationUtil.serializeToByteArray(original);
    assertEquals(12, bytes.length);
    bytes = SerializationUtil.serializeToByteArray(original, true);
    assertEquals(12, bytes.length);
    bytes = SerializationUtil.serializeToByteArray(original, true, CompressionType.NONE);
    assertEquals(49, bytes.length);

    bytes = SerializationUtil.serializeToByteArray(original, true, CompressionType.GZIP);
    assertEquals(57, bytes.length);
    bytes = SerializationUtil.serializeToByteArray(original);
    Object restored = SerializationUtil.deserializeFromByteArray(bytes);
    assertEquals(original, restored);
  }

  public void testSerializeToFromByteArray() throws Exception {
    Iterable<CompressionType> compressionTypes =
        Arrays.asList(CompressionType.NONE, CompressionType.GZIP, null);
    for (Serializable original : Arrays.asList(10L, "hello", CompressionType.GZIP)) {
      for (boolean ignoreHeader : Arrays.asList(true, false)) {
        for (CompressionType compression : compressionTypes) {
          byte[] bytes =
              SerializationUtil.serializeToByteArray(original, ignoreHeader, compression);
          Object restored = SerializationUtil.deserializeFromByteArray(bytes, ignoreHeader);
          assertEquals(original, restored);
          ByteBuffer buffer  = ByteBuffer.wrap(bytes);
          restored = SerializationUtil.deserializeFromByteBuffer(buffer, ignoreHeader);
          assertEquals(original, restored);
          bytes = SerializationUtil.serializeToByteArray(original, ignoreHeader);
          restored = SerializationUtil.deserializeFromByteArray(bytes, ignoreHeader);
          assertEquals(original, restored);
        }
      }
    }
  }
}
