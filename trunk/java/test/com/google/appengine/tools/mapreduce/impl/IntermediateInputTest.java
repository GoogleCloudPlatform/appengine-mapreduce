// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static java.util.Arrays.asList;

import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileServicePb.KeyValues;
import com.google.appengine.api.files.RecordReadChannel;
import com.google.appengine.repackaged.com.google.protobuf.ByteString;
import com.google.appengine.tools.development.testing.LocalFileServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.ReducerInput;
import com.google.appengine.tools.mapreduce.impl.IntermediateInput.Reader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import junit.framework.TestCase;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

/**
 * @author ohler@google.com (Christian Ohler)
 */
public class IntermediateInputTest extends TestCase {

  private static final Logger log = Logger.getLogger(IntermediateInputTest.class.getName());

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalFileServiceTestConfig());

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    helper.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

  private <K, V> void assertKeyValuesMatch(KeyValue<K, List<V>> expected,
      KeyValue<K, ReducerInput<V>> actual) {
    assertEquals("key", expected.getKey(), actual.getKey());
    assertEquals("values for key " + expected.getKey(),
        expected.getValue(), Lists.newArrayList(actual.getValue()));
  }

  private <K, V> void assertYieldsValues(List<KeyValue<K, List<V>>> expected, Reader<K, V> reader) {
    Iterator<KeyValue<K, List<V>>> in = expected.iterator();
    while (in.hasNext()) {
      KeyValue<K, List<V>> expectedNext = in.next();
      KeyValue<K, ReducerInput<V>> actualNext;
      try {
        actualNext = reader.next();
      } catch (NoSuchElementException e) {
        fail("Wanted " + expectedNext + ", got NoSuchElementException");
        throw new AssertionError();
      }
      assertKeyValuesMatch(expectedNext, actualNext);
    }
    try {
      Object actualNext = reader.next();
      fail("Wanted end of input, got " + actualNext);
    } catch (NoSuchElementException e) {
      // ok
    }
  }

  private static class MockChannel implements RecordReadChannel {
    private final List<KeyValues> records;
    private int pos = 0;

    MockChannel(KeyValues... records) {
      this.records = ImmutableList.copyOf(records);
      log.info("records=" + this.records);
    }

    @Override public ByteBuffer readRecord() throws IOException {
      if (pos >= records.size()) {
        throw new EOFException();
      }
      log.info("readRecord() at pos " + pos + " returning " + records.get(pos));
      return ByteBuffer.wrap(records.get(pos++).toByteArray());
    }

    @Override public long position() throws IOException {
      return pos;
    }

    @Override public void position(long newPosition) throws IOException {
      pos = Ints.checkedCast(newPosition);
    }
  }

  private static final Marshaller<Integer> MARSHALLER = Marshallers.getIntegerMarshaller();

  private KeyValues makeProto(int key, boolean partial, int... values) {
    KeyValues.Builder out = KeyValues.newBuilder()
        .setKey(ByteString.copyFrom(MARSHALLER.toBytes(key)))
        .setPartial(partial);
    for (int value : values) {
      out.addValue(ByteString.copyFrom(MARSHALLER.toBytes(value)));
    }
    return out.build();
  }

  public void testFoo() throws Exception {
    AppEngineFile dummyFile = new AppEngineFile("/blobstore/dummy-file-name");
    final RecordReadChannel mockChannel = new MockChannel(
        makeProto(0, false, 1),
        makeProto(15, false, 1, 1, 4, 1),
        makeProto(1, true, 2),
        makeProto(1, false, 0, 4),
        makeProto(-27, true, -1, -1),
        makeProto(-27, false, -1));
    Reader<Integer, Integer> reader = new Reader<Integer, Integer>(dummyFile,
        MARSHALLER, MARSHALLER) {
      @Override RecordReadChannel openChannel() {
        return mockChannel;
      }
    };
    assertYieldsValues(
        ImmutableList.of(KeyValue.of(0, asList(1)),
            KeyValue.of(15, asList(1, 1, 4, 1)),
            KeyValue.of(1, asList(2, 0, 4)),
            KeyValue.of(-27, asList(-1, -1, -1))),
        reader);
  }

}
