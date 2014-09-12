package com.google.appengine.tools.mapreduce.impl.sort;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;

import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MergeShardTaskTest extends TestCase {

  static class MockInputReader extends InputReader<KeyValue<ByteBuffer, Iterator<ByteBuffer>>> {
    private static final long serialVersionUID = 1L;

    @Override
    public KeyValue<ByteBuffer, Iterator<ByteBuffer>> next() {
      return null;
    }
  }

  static class MockOutputWriter extends OutputWriter<KeyValue<ByteBuffer, List<ByteBuffer>>> {
    private static final long serialVersionUID = 1L;
    List<Integer> written = new ArrayList<>();

    @Override
    public void write(KeyValue<ByteBuffer, List<ByteBuffer>> value) {
      written.add(value.getValue().size());
    }
  }

  public void testOutputSegmented() {
    MockOutputWriter writer = new MockOutputWriter();
    MergeShardTask task =
        new MergeShardTask("TestJob", 0, 1, new MockInputReader(), writer, Integer.MAX_VALUE);
    task.callWorker(createData(1));
    assertEquals(1, writer.written.size());
    task.callWorker(createData(3));
    assertEquals(2, writer.written.size());
    assertEquals(1, (int) writer.written.get(0));
    assertEquals(3, (int) writer.written.get(1));
    writer.written.clear();
    for (int i = 0; i < 32; i++) {
      task.callWorker(createData(32));
    }
    assertEquals(32, writer.written.size());
    for (int i = 0; i < 32; i++) {
      assertEquals((int) writer.written.get(i), 32);
    }
  }

  public void testSerialization() {
    MergeShardTask task =
        new MergeShardTask("TestJob", 0, 1, new MockInputReader(), new MockOutputWriter(), 0);

    task.callWorker(createData(1));
    assertEquals(1, ((MockOutputWriter) task.getOutputWriter()).written.size());
    task = SerializationUtil.clone(task);

    task.callWorker(createData(1));
    assertEquals(2, ((MockOutputWriter) task.getOutputWriter()).written.size());
  }

  private KeyValue<ByteBuffer, Iterator<ByteBuffer>> createData(int numValues) {
    List<ByteBuffer> values = new ArrayList<>(numValues);
    for (int i = 0; i < numValues; i++) {
      values.add(ByteBuffer.allocate(0));
    }
    return new KeyValue<>(ByteBuffer.allocate(0), values.iterator());
  }
}
