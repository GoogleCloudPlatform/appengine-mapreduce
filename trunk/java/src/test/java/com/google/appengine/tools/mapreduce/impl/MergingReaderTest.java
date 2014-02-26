package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.inputs.PeekingInputReader;
import com.google.common.collect.Iterators;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Tests for {@link MergingReader}
 */
public class MergingReaderTest extends TestCase {

  private static class StaticInputReader extends InputReader<ByteBuffer> {

    private static final long serialVersionUID = 1L;
    private final ArrayList<String> keys;
    private final ArrayList<List<Integer>> valueSets;
    private final KeyValuesMarshaller<String, Integer> marshaller;
    private int offset = 0;

    public StaticInputReader(LinkedHashMap<String, List<Integer>> results) {
      super();
      marshaller = new KeyValuesMarshaller<>(Marshallers.getStringMarshaller(),
          Marshallers.getIntegerMarshaller());
      keys = new ArrayList<>(results.keySet());
      valueSets = new ArrayList<>(results.values());
  }

    @Override
    public ByteBuffer next() throws NoSuchElementException {
      if (offset >= keys.size()) {
        throw new NoSuchElementException();
      }
      ByteBuffer result = marshaller.toBytes(
          new KeyValue<>(keys.get(offset), valueSets.get(offset).iterator()));
      offset++;
      return result;
    }

    @Override
    public Double getProgress() {
      return null;
    }
  }

  public void testNoReaders() throws IOException {
    List<PeekingInputReader<KeyValue<ByteBuffer, Iterator<Long>>>> readers = new ArrayList<>();
    MergingReader<String, Long> merging =
        new MergingReader<>(readers, Marshallers.getStringMarshaller());
    merging.beginSlice();
    try {
      KeyValue<String, Iterator<Long>> next = merging.next();
      fail(String.valueOf(next));
    } catch (NoSuchElementException e) {
      // expected
    }
  }

  public void testReaderWithEmptyIteraors() throws IOException {
    List<PeekingInputReader<KeyValue<ByteBuffer, Iterator<Integer>>>> readers = new ArrayList<>();
    int numKeys = 2;
    readers.add(createReader(numKeys, 1));
    readers.add(createReader(numKeys, 0));
    MergingReader<String, Integer> merging =
        new MergingReader<>(readers, Marshallers.getStringMarshaller());
    merging.beginSlice();
    for (int key = 0; key < numKeys; key++) {
      KeyValue<String, Iterator<Integer>> next = merging.next();
      assertEquals(String.valueOf(key), next.getKey());
      Iterator<Integer> iter = next.getValue();
      assertEquals(Integer.valueOf(0), iter.next());
      assertFalse(iter.hasNext());
    }
  }

  public void testOneReader() throws IOException {
    List<PeekingInputReader<KeyValue<ByteBuffer, Iterator<Integer>>>> readers = new ArrayList<>();
    int numKeys = 10;
    int valuesPerKey = 10;

    readers.add(createReader(numKeys, valuesPerKey));
    MergingReader<String, Integer> merging =
        new MergingReader<>(readers, Marshallers.getStringMarshaller());
    merging.beginSlice();
    int valueCount = 0;
    for (int key = 0; key < numKeys; key++) {
      KeyValue<String, Iterator<Integer>> next = merging.next();
      assertEquals(String.valueOf(key), next.getKey());
      valueCount += Iterators.toArray(next.getValue(), Integer.class).length;
    }
    assertEquals(numKeys * valuesPerKey, valueCount);
    try {
      merging.next();
      fail();
    } catch (NoSuchElementException e) {
      // expected
    }
  }

  public void testMultipleReaders() throws IOException {
    List<PeekingInputReader<KeyValue<ByteBuffer, Iterator<Integer>>>> readers = new ArrayList<>();
    int readerCount = 10;
    int numKeys = 10;
    int valuesPerKey = 10;

    for (int r = 0; r < readerCount; r++) {
      readers.add(createReader(numKeys, valuesPerKey));
    }
    MergingReader<String, Integer> merging =
        new MergingReader<>(readers, Marshallers.getStringMarshaller());
    verifyExpectedOutput(readerCount, numKeys, valuesPerKey, merging);
  }

  public void testSerializingMultipleReaders() throws IOException, ClassNotFoundException {
    List<PeekingInputReader<KeyValue<ByteBuffer, Iterator<Integer>>>> readers = new ArrayList<>();
    int readerCount = 10;
    int numKeys = 10;
    int valuesPerKey = 10;

    for (int r = 0; r < readerCount; r++) {
      readers.add(createReader(numKeys, valuesPerKey));
    }
    MergingReader<String, Integer> merging =
        new MergingReader<>(readers, Marshallers.getStringMarshaller());
    merging.beginSlice();
    int valueCount = 0;
    for (int key = 0; key < numKeys; key++) {
      merging.endSlice();
      merging = reconstruct(merging);
      merging.beginSlice();
      KeyValue<String, Iterator<Integer>> next = merging.next();
      assertEquals(String.valueOf(key), next.getKey());
      valueCount += Iterators.toArray(next.getValue(), Integer.class).length;
    }
    assertEquals(readerCount * numKeys * valuesPerKey, valueCount);
    try {
      merging.next();
      fail();
    } catch (NoSuchElementException e) {
      // expected
    }
    merging.endSlice();
    merging = reconstruct(merging);
    merging.beginSlice();
    try {
      merging.next();
      fail();
    } catch (NoSuchElementException e) {
      // expected
    }
  }

  public void testSerializingIgnoringValues() throws IOException, ClassNotFoundException {
    List<PeekingInputReader<KeyValue<ByteBuffer, Iterator<Integer>>>> readers = new ArrayList<>();
    int readerCount = 10;
    int numKeys = 10;
    int valuesPerKey = 100;

    for (int r = 0; r < readerCount; r++) {
      readers.add(createReader(numKeys, valuesPerKey));
    }
    MergingReader<String, Integer> merging =
        new MergingReader<>(readers, Marshallers.getStringMarshaller());
    merging.beginSlice();
    for (int key = 0; key < numKeys; key++) {
      merging.endSlice();
      merging = reconstruct(merging);
      merging.beginSlice();
      KeyValue<String, Iterator<Integer>> next = merging.next();
      assertEquals(String.valueOf(key), next.getKey());
    }
    try {
      merging.next();
      fail();
    } catch (NoSuchElementException e) {
      // expected
    }
    merging.endSlice();
    merging = reconstruct(merging);
    merging.beginSlice();
    try {
      merging.next();
      fail();
    } catch (NoSuchElementException e) {
      // expected
    }
  }

  private MergingReader<String, Integer> reconstruct(
      MergingReader<String, Integer> reader) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
      oout.writeObject(reader);
    }
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bout.toByteArray()));
    return (MergingReader<String, Integer>) in.readObject();
  }

  private void verifyExpectedOutput(int readerCount, int numKeys, int valuesPerKey,
      MergingReader<String, Integer> merging) throws IOException {
    merging.beginSlice();
    int valueCount = 0;
    for (int key = 0; key < numKeys; key++) {
      KeyValue<String, Iterator<Integer>> next = merging.next();
      assertEquals(String.valueOf(key), next.getKey());
      valueCount += Iterators.toArray(next.getValue(), Integer.class).length;
    }
    assertEquals(readerCount * numKeys * valuesPerKey, valueCount);
    try {
      merging.next();
      fail();
    } catch (NoSuchElementException e) {
      // expected
    }
  }

  private PeekingInputReader<KeyValue<ByteBuffer, Iterator<Integer>>> createReader(int keys,
      int valuesPerKey) {
    KeyValuesMarshaller<ByteBuffer, Integer> inMarshaller = new KeyValuesMarshaller<>(
        Marshallers.getByteBufferMarshaller(), Marshallers.getIntegerMarshaller());
    StaticInputReader staticInputReader =
        new StaticInputReader(createSampleInput(keys, valuesPerKey));
    PeekingInputReader<KeyValue<ByteBuffer, Iterator<Integer>>> reader =
        new PeekingInputReader<>(staticInputReader, inMarshaller);
    return reader;
  }

  private LinkedHashMap<String, List<Integer>> createSampleInput(int keys, int valuesPerKey) {
    LinkedHashMap<String, List<Integer>> result = new LinkedHashMap<>();
    for (int key = 0; key < keys; key++) {
      List<Integer> values = new ArrayList<>();
      for (int i = 0; i < valuesPerKey; i++) {
        values.add(i);
      }
      result.put(String.valueOf(key), values);
    }
    return result;
  }
}
