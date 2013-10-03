package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.inputs.InMemoryInput;
import com.google.common.collect.ImmutableList;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Validates that data written by the InMemoryOutputWriter and InMemoryInputReader pass along the
 * data as is.
 *
 */
public class InMemoryInputOutputTest extends TestCase {

  public void testReaderWriter() throws IOException, ClassNotFoundException {
    InMemoryOutput<Object> output = new InMemoryOutput<Object>(1);
    Collection<? extends OutputWriter<Object>> writers = output.createWriters();
    assertEquals(1, writers.size());
    OutputWriter<Object> writer = writers.iterator().next();
    String one = "one";
    String two = "two";
    writer.open();
    writer.beginSlice();
    writer.write(one);
    writer.endSlice();
    writer = reconstruct(writer);
    writer.beginSlice();
    writer.write(two);
    writer.endSlice();
    writer.close();
    List<List<Object>> data = output.finish(ImmutableList.of(writer));
    InMemoryInput<Object> input = new InMemoryInput<Object>(data);
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

  @SuppressWarnings("unchecked")
  private OutputWriter<Object> reconstruct(OutputWriter<Object> writer) throws IOException,
      ClassNotFoundException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ObjectOutputStream oout = new ObjectOutputStream(bout);
    oout.writeObject(writer);
    oout.close();
    ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
    ObjectInputStream oin = new ObjectInputStream(bin);
    return (OutputWriter<Object>) oin.readObject();
  }

  public void testManyShards() throws IOException {
    int numShards = 10;
    InMemoryOutput<Object> output = new InMemoryOutput<Object>(numShards);
    assertEquals(numShards, output.getNumShards());

    Collection<? extends OutputWriter<Object>> writers = output.createWriters();
    assertEquals(numShards, writers.size());

    List<List<Object>> data = output.finish(writers);

    InMemoryInput<Object> input = new InMemoryInput<Object>(data);
    List<? extends InputReader<Object>> readers = input.createReaders();
    assertEquals(numShards, readers.size());

  }

}
