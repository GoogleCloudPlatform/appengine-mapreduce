// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.util;

import static java.util.Arrays.asList;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil.CompressionType;

import junit.framework.TestCase;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author ohler@google.com (Christian Ohler)
 */
public class SerializationUtilTest extends TestCase {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper();
  private DatastoreService datastore;

  @Override
  protected void setUp() throws Exception {
    helper.setUp();
    datastore = DatastoreServiceFactory.getDatastoreService();
  }

  @Override
  protected void tearDown() throws Exception {
    helper.tearDown();
  }

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
        asList(CompressionType.NONE, CompressionType.GZIP, null);
    for (Serializable original : asList(10L, "hello", new Value(1000), CompressionType.GZIP)) {
      for (boolean ignoreHeader : asList(true, false)) {
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

  private static class Value implements Serializable {

    private static final long serialVersionUID = -2908491492725087639L;
    private byte[] bytes;

    Value(int kb) {
      bytes = new byte[kb * 1024];
      new Random().nextBytes(bytes);
    }

    @Override
    public int hashCode() {
      return ByteBuffer.wrap(bytes).getInt();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Value) {
        Value other = (Value) obj;
        return Arrays.equals(bytes, other.bytes);
      }
      return false;
    }
  }

  public void testSerializeToDatastore() throws Exception {
    Key key = KeyFactory.createKey("mr-entity", 1);
    List<Value> values = asList(null, new Value(0), new Value(500), new Value(2000),
        new Value(10000), new Value(1500));
    Iterable<CompressionType> compressionTypes =
        asList(CompressionType.NONE, CompressionType.GZIP, null);
    for (Value original : values) {
      for (CompressionType compression : compressionTypes) {
        Transaction tx = datastore.beginTransaction();
        Entity entity = new Entity(key);
        SerializationUtil.serializeToDatastoreProperty(tx, entity, "foo", original, compression);
        datastore.put(entity);
        tx.commit();
        entity = datastore.get(key);
        Serializable restored = SerializationUtil.deserializeFromDatastoreProperty(entity, "foo");
        assertEquals(original, restored);
      }
    }
  }
}
