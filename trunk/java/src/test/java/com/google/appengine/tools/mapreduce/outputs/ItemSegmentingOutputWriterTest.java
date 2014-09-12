package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ItemSegmentingOutputWriterTest extends TestCase {

  private static class CountingMockWriter extends OutputWriter<Integer> {
    private static final long serialVersionUID = 1L;
    int beginShard = 0;
    int endShard = 0;
    int beginSlice = 0;
    int endSlice = 0;
    int write = 0;

    @Override
    public void beginSlice() {
      beginSlice++;
    }

    @Override
    public void endSlice() {
      endSlice++;
    }

    @Override
    public void write(Integer value) {
      write++;
    }

    @Override
    public void beginShard() {
      beginShard++;
    }

    @Override
    public void endShard(){
      endShard++;
    }

    @SuppressWarnings("hiding")
    private void assertValues(int beginShard, int beginSlice, int write, int endSlice,
        int endShard) {
      assertEquals(beginShard, this.beginShard);
      assertEquals(beginSlice, this.beginSlice);
      assertEquals(write, this.write);
      assertEquals(endSlice, this.endSlice);
      assertEquals(endShard, this.endShard);
    }
  }

  private static class CountingMockWriterCreator extends ItemSegmentingOutputWriter<Integer> {
    private static final long serialVersionUID = 1L;
    List<CountingMockWriter> created = new ArrayList<>();
    Integer lastItem;

    @Override
    public CountingMockWriter createNextWriter(int number) {
      CountingMockWriter writer = new CountingMockWriter();
      created.add(writer);
      return writer;
    }

    @Override
    public long estimateMemoryRequirement() {
      return 0;
    }

    @Override
    public boolean shouldSegment(Integer value) {
      boolean result = lastItem != null && lastItem.compareTo(value) > 0;
      lastItem = value;
      return result;
    }
  }

  public void testMethodsPassThrough() throws IOException {
    CountingMockWriterCreator writer = new CountingMockWriterCreator();
    writer.beginShard();
    assertEquals(1, writer.created.size());
    writer.created.get(0).assertValues(1, 0, 0, 0, 0);
    writer.beginSlice();
    assertEquals(1, writer.created.size());
    writer.created.get(0).assertValues(1, 1, 0, 0, 0);
    writer.write(1);
    assertEquals(1, writer.created.size());
    writer.created.get(0).assertValues(1, 1, 1, 0, 0);
    writer.write(2);
    assertEquals(1, writer.created.size());
    writer.created.get(0).assertValues(1, 1, 2, 0, 0);
    writer.endSlice();
    assertEquals(1, writer.created.size());
    writer.created.get(0).assertValues(1, 1, 2, 1, 0);
    writer.endShard();
    assertEquals(1, writer.created.size());
    writer.created.get(0).assertValues(1, 1, 2, 1, 1);
  }

  public void testSegmentation() throws IOException {
    CountingMockWriterCreator writer = new CountingMockWriterCreator();
    writer.beginShard();
    writer.beginSlice();
    writer.write(3);
    writer.write(2);
    assertEquals(2, writer.created.size());
    writer.created.get(0).assertValues(1, 1, 1, 1, 1);
    writer.created.get(1).assertValues(1, 1, 1, 0, 0);
    writer.write(3);
    writer.write(2);
    writer.endSlice();
    writer.endShard();
    assertEquals(3, writer.created.size());
    writer.created.get(0).assertValues(1, 1, 1, 1, 1);
    writer.created.get(1).assertValues(1, 1, 2, 1, 1);
    writer.created.get(2).assertValues(1, 1, 1, 1, 1);
  }

  public void testSlicing() throws IOException {
    CountingMockWriterCreator writer = new CountingMockWriterCreator();
    writer.beginShard();
    writer.beginSlice();
    writer.write(3);
    writer.write(2);
    assertEquals(2, writer.created.size());
    writer.endSlice();
    writer = SerializationUtil.clone(writer);
    writer.beginSlice();
    writer.created.get(0).assertValues(1, 1, 1, 1, 1);
    writer.created.get(1).assertValues(1, 2, 1, 1, 0);
    writer.endSlice();
    writer = SerializationUtil.clone(writer);
    writer.beginSlice();
    writer.write(3);
    writer.created.get(0).assertValues(1, 1, 1, 1, 1);
    writer.created.get(1).assertValues(1, 3, 2, 2, 0);
    writer.write(2);
    writer.write(2);
    writer.endSlice();
    writer.endShard();
    assertEquals(3, writer.created.size());
    writer.created.get(0).assertValues(1, 1, 1, 1, 1);
    writer.created.get(1).assertValues(1, 3, 2, 3, 1);
    writer.created.get(2).assertValues(1, 1, 2, 1, 1);
  }
}
