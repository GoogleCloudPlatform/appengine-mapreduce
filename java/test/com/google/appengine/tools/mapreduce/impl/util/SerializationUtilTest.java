// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.util;

import junit.framework.TestCase;

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

}
