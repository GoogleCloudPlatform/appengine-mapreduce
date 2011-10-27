// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.files.FileServicePb.KeyValue;

import junit.framework.TestCase;

import java.io.IOException;

/**
 * Tests the {@link ObjectReader} and {@link ObjectWriter}
 *
 */
public class ObjectReaderWriterTest extends TestCase {



  /**
   * Useful tester class for setting up the reader and writer.
   * @param <T>
   */
  private class ObjectIOTester<T> {
    private ObjectWriter<T> writer;
    private ObjectReader<T> reader;

    public ObjectIOTester(Serializer<T> encoder) {
      MockRecordChannel mc = new MockRecordChannel();
      writer = new ObjectWriter<T>(encoder, mc);
      reader = new ObjectReader<T>(encoder, mc);
    }

    public T read() throws IOException {
      return reader.read();
    }
    public void write(T object) throws IOException {
      writer.write(object);
    }
  }

  /**
   * Tests writing and reading a string.
   */
  public void testReadWriteString() throws Exception {
    ObjectIOTester<String> test = new ObjectIOTester<String>(Serializers.strings());
    String str = "Hello, world!";
    test.write(str);
    assertEquals(str, test.read());
  }


  /**
   * Tests reading and writing a {@link KeyValue} Protocol Buffer.
   */
  public void testReadWriteKeyValue() throws Exception {
    ObjectIOTester<KeyValue> test = new ObjectIOTester<KeyValue>(
        new Serializers.KeyValueEncoder());
    KeyValue kv = KeyValue.newBuilder().setKey("key")
                                       .setValue("value")
                                       .build();
    test.write(kv);
    assertEquals(kv, test.read());
  }

}
