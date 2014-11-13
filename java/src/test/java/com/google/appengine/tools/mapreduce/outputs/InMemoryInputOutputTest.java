package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.appengine.tools.mapreduce.inputs.InMemoryInput;
import com.google.common.collect.ImmutableList;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Validates that data written by the InMemoryOutputWriter and InMemoryInputReader pass along the
 * data as is.
 *
 */
public class InMemoryInputOutputTest extends TestCase {

  public void testReaderWriter() throws IOException {
    InMemoryOutput<Object> output = new InMemoryOutput<>();
    Collection<? extends OutputWriter<Object>> writers = output.createWriters(1);
    assertEquals(1, writers.size());
    OutputWriter<Object> writer = writers.iterator().next();
    String one = "one";
    String two = "two";
    writer.beginShard();
    writer.beginSlice();
    writer.write(one);
    writer.endSlice();
    writer = SerializationUtil.clone(writer);
    writer.beginSlice();
    writer.write(two);
    writer.endSlice();
    writer.endShard();
    List<List<Object>> data = output.finish(ImmutableList.of(writer));
    InMemoryInput<Object> input = new InMemoryInput<>(data);
    List<? extends InputReader<Object>> readers = input.createReaders();
    assertEquals(1, readers.size());
    InputReader<Object> reader = readers.get(0);
    reader.beginSlice();
    assertEquals(0.0, reader.getProgress());
    assertEquals(one, reader.next());
    assertSame(two, reader.next());
    assertEquals(1.0, reader.getProgress());
    try {
      reader.next();
    } catch (NoSuchElementException e) {
      // expected
    }
    reader.endSlice();
  }

  public void testManyShards() {
    int numShards = 10;
    InMemoryOutput<Object> output = new InMemoryOutput<>();

    Collection<? extends OutputWriter<Object>> writers = output.createWriters(numShards);
    assertEquals(numShards, writers.size());

    List<List<Object>> data = output.finish(writers);

    InMemoryInput<Object> input = new InMemoryInput<>(data);
    List<? extends InputReader<Object>> readers = input.createReaders();
    assertEquals(numShards, readers.size());
  }
}
