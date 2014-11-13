package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Sharder;
import com.google.appengine.tools.mapreduce.impl.HashingSharder;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;


public class ShardingOutputWriterTest extends TestCase {

  private static final class TestShardingOutputWriter extends
      ShardingOutputWriter<Integer, Integer, OutputWriter<KeyValue<Integer, Integer>>> {
    private static final long serialVersionUID = 1L;
    int shardsCreated = 0;

    private TestShardingOutputWriter(Marshaller<Integer> keyMarshaller, Sharder sharder) {
      super(keyMarshaller, sharder);
    }

    @Override
    public OutputWriter<KeyValue<Integer, Integer>> createWriter(int number) {
      shardsCreated++;
      return new InMemoryOutputWriter<>();
    }

    @Override
    public long estimateMemoryRequirement() {
      return 7;
    }
  }

  public void testCreatorCalled() throws IOException {
    int numShards = 10;
    TestShardingOutputWriter writer = new TestShardingOutputWriter(
        Marshallers.getIntegerMarshaller(), new HashingSharder(numShards));
    writer.beginShard();
    writer.beginSlice();
    for (int i = 0; i < numShards * 10; i++) {
      writer.write(new KeyValue<>(i, i));
    }
    assertEquals(numShards, writer.shardsCreated);
    writer.endSlice();
    writer = SerializationUtil.clone(writer);
    writer.beginSlice();
    for (int i = 0; i < numShards * 10; i++) {
      writer.write(new KeyValue<>(i, i));
    }
    assertEquals(numShards, writer.shardsCreated);
    writer.endSlice();
    writer.endShard();
    assertEquals(numShards, writer.shardsCreated);
  }

  public void testMethodsCalled() throws IOException {
    int numShards = 10;
    final AtomicInteger itemsWritten = new AtomicInteger(0);
    final AtomicInteger shardBegins = new AtomicInteger(0);
    final AtomicInteger shardEnds = new AtomicInteger(0);
    final AtomicInteger sliceBegins = new AtomicInteger(0);
    final AtomicInteger sliceEnds = new AtomicInteger(0);
    ShardingOutputWriter<Integer, Integer, OutputWriter<KeyValue<Integer, Integer>>> writer =
        new ShardingOutputWriter<Integer, Integer, OutputWriter<KeyValue<Integer, Integer>>>(
            Marshallers.getIntegerMarshaller(), new HashingSharder(numShards)) {
          private static final long serialVersionUID = 1L;

          @Override
          public OutputWriter<KeyValue<Integer, Integer>> createWriter(int number) {
            return new InMemoryOutputWriter<KeyValue<Integer, Integer>>() {
              private static final long serialVersionUID = 1L;

              @Override
              public void beginShard() {
                super.beginShard();
                shardBegins.incrementAndGet();
              }

              @Override
              public void beginSlice() {
                super.beginSlice();
                sliceBegins.incrementAndGet();
              }

              @Override
              public void endSlice() {
                super.endSlice();
                sliceEnds.incrementAndGet();
              }

              @Override
              public void endShard() {
                super.endShard();
                shardEnds.incrementAndGet();
              }

              @Override
              public void write(KeyValue<Integer, Integer> foo) {
                super.write(foo);
                itemsWritten.incrementAndGet();
              }
            };
          }

          @Override
          public long estimateMemoryRequirement() {
            return 0;
          }
        };
    writer.beginShard();
    writer.beginSlice();
    for (int i = 0; i < numShards * 10; i++) {
      writer.write(new KeyValue<>(i, i));
    }
    writer.endSlice();
    writer.beginSlice();
    for (int i = 0; i < numShards * 10; i++) {
      writer.write(new KeyValue<>(i, i));
    }
    writer.endSlice();
    writer.endShard();
    assertEquals(numShards * 10 * 2, itemsWritten.get());
    assertEquals(numShards, shardBegins.get());
    assertEquals(numShards, shardEnds.get());
    assertEquals(numShards * 2, sliceBegins.get());
    assertEquals(numShards * 2, sliceEnds.get());
  }
}
