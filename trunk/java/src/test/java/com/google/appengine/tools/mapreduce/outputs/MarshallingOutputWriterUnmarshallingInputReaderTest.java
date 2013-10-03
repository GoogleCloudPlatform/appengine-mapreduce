package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.inputs.InMemoryInput;
import com.google.appengine.tools.mapreduce.inputs.UnmarshallingInput;

import junit.framework.TestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Validates that data written by the marshallingOutputWriter can be read by the
 * unmarshallingInputWriter
 *
 */
public class MarshallingOutputWriterUnmarshallingInputReaderTest extends TestCase {

  public void testReaderWriter() throws IOException {
    Marshaller<String> stringMarshaller = Marshallers.getStringMarshaller();
    MarshallingOutput<String, List<List<ByteBuffer>>> output = new MarshallingOutput<String,
        List<List<ByteBuffer>>>(new InMemoryOutput<ByteBuffer>(1), stringMarshaller);
    Collection<MarshallingOutputWriter<String>> writers = output.createWriters();
    assertEquals(1, writers.size());
    MarshallingOutputWriter<String> writer = writers.iterator().next();
    writer.open();
    writer.beginSlice();
    writer.write("Foo");
    writer.write("Bar");
    writer.endSlice();
    writer.beginSlice();
    writer.write("Baz");
    writer.close();
    List<List<ByteBuffer>> data = output.finish(writers);
    UnmarshallingInput<String> input =
        new UnmarshallingInput<String>(new InMemoryInput<ByteBuffer>(data), stringMarshaller);
    List<InputReader<String>> readers = input.createReaders();
    assertEquals(1, readers.size());
    InputReader<String> reader = readers.get(0);
    reader.beginSlice();
    assertEquals(0.0, reader.getProgress());
    assertEquals("Foo", reader.next());
    assertEquals("Bar", reader.next());
    assertEquals("Baz", reader.next());
    assertEquals(1.0, reader.getProgress());
    try {
      reader.next();
    } catch (NoSuchElementException e) {
      // expected
    }
    reader.endSlice();
  }

  public void testInputOutput() throws IOException {
    int numShards = 10;
    Marshaller<String> stringMarshaller = Marshallers.getStringMarshaller();
    MarshallingOutput<String, List<List<ByteBuffer>>> output = new MarshallingOutput<String,
        List<List<ByteBuffer>>>(new InMemoryOutput<ByteBuffer>(numShards), stringMarshaller);
    assertEquals(numShards, output.getNumShards());
    Collection<MarshallingOutputWriter<String>> writers = output.createWriters();
    assertEquals(numShards, writers.size());
    List<List<ByteBuffer>> data = output.finish(writers);

    UnmarshallingInput<String> input =
        new UnmarshallingInput<String>(new InMemoryInput<ByteBuffer>(data), stringMarshaller);
    List<InputReader<String>> readers = input.createReaders();
    assertEquals(numShards, readers.size());
  }

}
