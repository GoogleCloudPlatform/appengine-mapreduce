package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.mapreduce.impl.InProcessMap;
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
public class MapSpecificationTest extends TestCase {

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
  private static class MapOnlyMapper1 extends MapOnlyMapper<Number, Integer> {

    @Override
    public void map(Number value) {
      emit(value.intValue());
    }
  }

  @SuppressWarnings("serial")
  private static class MapOnlyMapper2 extends MapOnlyMapper<Long, Void> {

    @Override
    public void map(Long value) {
    }
  }

  @SuppressWarnings("serial")
  private static class MapOnlyMapper3 extends MapOnlyMapper<Long, Number> {

    @Override
    public void map(Long value) {
      emit(value.intValue());
    }
  }

  @SuppressWarnings("serial")
  private static class Mapper1 extends Mapper<Number, Void, Integer> {

    @Override
    public void map(Number value) {
      emit(null, value.intValue());
    }
  }

  @SuppressWarnings("serial")
  private static class Mapper2 extends Mapper<Long, Void, Void> {

    @Override
    public void map(Long value) {
    }
  }

  @SuppressWarnings("serial")
  private static class Mapper3 extends Mapper<Long, Void, Number> {

    @Override
    public void map(Long value) {
      emit(null, value.intValue());
    }
  }

  public void testDefaultConstructor() throws Exception {
    MapSpecification.Builder<Number, Integer, Number> builder =
        new MapSpecification.Builder<Number, Integer, Number>()
            .setJobName("bla")
            .setInput(new Input1())
            .setMapper(new MapOnlyMapper1())
            .setOutput(new Output1());
    MapSpecification<Number, Integer, Number> spec = builder.build();
    Number result = InProcessMap.runMap(spec).getOutputResult();
    assertEquals(Integer.class, result.getClass());
    assertEquals(15, result.intValue());

    MapOnlyMapper<Number, Integer> mapper = MapOnlyMapper.forMapper(new Mapper1());
    builder = new MapSpecification.Builder<Number, Integer, Number>()
            .setJobName("bla")
            .setInput(new Input1())
            .setMapper(mapper)
            .setOutput(new Output1());
    spec = builder.build();
    result = InProcessMap.runMap(spec).getOutputResult();
    assertEquals(Integer.class, result.getClass());
    assertEquals(15, result.intValue());
  }

  public void testConstructorForMapWithNoOuput() throws Exception {
    MapSpecification.Builder<Long, Void, Void> builder =
        new MapSpecification.Builder<>(new Input1(), new MapOnlyMapper2());
    MapSpecification<Long, Void, Void> spec = builder.build();
    Void result = InProcessMap.runMap(spec).getOutputResult();
    assertNull(result);

    MapOnlyMapper<Long, Void> mapper = MapOnlyMapper.forMapper(new Mapper2());
    builder = new MapSpecification.Builder<>(new Input1(), mapper);
    spec = builder.build();
    result = InProcessMap.runMap(spec).getOutputResult();
    assertNull(result);
  }

  public void testConstructorForMapWithOuput() throws Exception {
    MapSpecification.Builder<Long, Number, Integer> builder =
        new MapSpecification.Builder<>(new Input1(), new MapOnlyMapper3(), new Output1());
    MapSpecification<Long, Number, Integer> spec = builder.build();
    Integer result = InProcessMap.runMap(spec).getOutputResult();
    assertEquals(15, result.intValue());

    MapOnlyMapper<Long, Number> mapper = MapOnlyMapper.forMapper(new Mapper3());
    builder = new MapSpecification.Builder<>(new Input1(), mapper, new Output1());
    spec = builder.build();
    result = InProcessMap.runMap(spec).getOutputResult();
    assertEquals(15, result.intValue());
  }
}
