// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;


import com.google.appengine.tools.mapreduce.impl.InProcessMapReduce;
import com.google.common.collect.ImmutableList;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

/**
 */
public class MapReduceSpecificationTest extends TestCase {

  @SuppressWarnings("serial")
  private static class InputReader1 extends InputReader<Long> {

    private final List<Integer> values = Arrays.asList(1, 2, 3, 4, 5);
    private int index = -1;

    @Override
    public void beginShard() throws IOException {
      index = 0;
    }

    @Override
    public Long next() throws NoSuchElementException {
      if (index < values.size()) {
        return Long.valueOf(values.get(index++));
      }
      throw new NoSuchElementException();
    }
  }

  @SuppressWarnings("serial")
  private static class Input1 extends Input<Long> {
    @Override
    public List<InputReader1> createReaders() {
      return ImmutableList.of(new InputReader1());
    }
  }

  @SuppressWarnings("serial")
  private static class OutputWriter1 extends OutputWriter<Number> {
    private int total;

    @Override
    public void write(Number value) throws IOException {
      total += value.intValue();
    }
  }

  @SuppressWarnings("serial")
  private static class Output1 extends Output<Number, Integer> {

    @Override
    public List<OutputWriter1> createWriters(int numShards) {
      List<OutputWriter1> writers = new ArrayList<>();
      for (int i = 0; i < numShards; i++) {
        writers.add(new OutputWriter1());
      }
      return writers;
    }

    @Override
    public Integer finish(Collection<? extends OutputWriter<Number>> writers) {
      int total = 0;
      for (OutputWriter<Number> writer : writers) {
        total += ((OutputWriter1) writer).total;
      }
      return total;
    }
  }

  @SuppressWarnings("serial")
  private static class Mapper1 extends Mapper<Number, Short, Integer> {

    @Override
    public void map(Number value) {
      emit((short) (value.intValue() % 2), value.intValue());
    }
  }

  @SuppressWarnings("serial")
  private static class Mapper2 extends Mapper<Long, Short, Integer> {

    @Override
    public void map(Long value) {
      emit((short) (value.intValue() % 2), value.intValue());
    }
  }

  @SuppressWarnings("serial")
  private static class Reducer1 extends Reducer<Short, Integer, Integer> {
    @Override
    public void reduce(Short key, ReducerInput<Integer> values) {
      int max = Integer.MIN_VALUE;
      while (values.hasNext()) {
        int value = values.next();
        if (value > max) {
          max = value;
        }
      }
      emit(max);
    }
  }

  @SuppressWarnings("serial")
  private static class Reducer2 extends Reducer<Short, Integer, Number> {
    @Override
    public void reduce(Short key, ReducerInput<Integer> values) {
      int max = Integer.MIN_VALUE;
      while (values.hasNext()) {
        int value = values.next();
        if (value > max) {
          max = value;
        }
      }
      emit(max);
    }
  }

  public void testDefaultConstructor() throws Exception {
    MapReduceSpecification.Builder<Number, Short, Integer, Integer, Number> builder =
        new MapReduceSpecification.Builder<Number, Short, Integer, Integer, Number>()
            .setJobName("bla")
            .setInput(new Input1())
            .setMapper(new Mapper1())
            .setKeyMarshaller(Marshallers.<Short>getSerializationMarshaller())
            .setValueMarshaller(Marshallers.<Integer>getSerializationMarshaller())
            .setReducer(new Reducer1())
            .setOutput(new Output1())
            .setNumReducers(1);
    MapReduceSpecification<Number, Short, Integer, Integer, Number> spec = builder.build();
    Number result = InProcessMapReduce.runMapReduce(spec).getOutputResult();
    assertEquals(Integer.class, result.getClass());
    assertEquals(9, result.intValue());

    builder = new MapReduceSpecification.Builder<Number, Short, Integer, Integer, Number>()
            .setInput(new Input1())
            .setMapper(new Mapper1())
            .setReducer(new Reducer1())
            .setOutput(new Output1())
            .setNumReducers(1);
    spec = builder.build();
    result = InProcessMapReduce.runMapReduce(spec).getOutputResult();
    assertEquals(Integer.class, result.getClass());
    assertEquals(9, result.intValue());
  }

  public void testConstructorWithValues() throws Exception {
    MapReduceSpecification.Builder<Long, Short, Integer, Number, Integer> builder =
        new MapReduceSpecification.Builder<>(new Input1(), new Mapper2(), new Reducer2(),
            new Output1()).setJobName("bla")
            .setKeyMarshaller(Marshallers.<Short>getSerializationMarshaller())
            .setValueMarshaller(Marshallers.<Integer>getSerializationMarshaller())
            .setNumReducers(1);
    MapReduceSpecification<Long, Short, Integer, Number, Integer> spec = builder.build();
    Number result = InProcessMapReduce.runMapReduce(spec).getOutputResult();
    assertEquals(Integer.class, result.getClass());
    assertEquals(9, result.intValue());
  }

  public void testConstructorWithValuesSkipOptionalValues() throws Exception {
    MapReduceSpecification.Builder<Long, Short, Integer, Number, Integer> builder =
        new MapReduceSpecification.Builder<>(new Input1(), new Mapper2(), new Reducer2(),
            new Output1());
    MapReduceSpecification<Long, Short, Integer, Number, Integer> spec = builder.build();
    Number result = InProcessMapReduce.runMapReduce(spec).getOutputResult();
    assertEquals(Integer.class, result.getClass());
    assertEquals(9, result.intValue());
  }
}
