// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

/**
 * Map function for Map only jobs.  A map function processes input
 * values one at a time and generates zero or more output values.
 *
 * <p>This class is really an interface that might be evolving. In order to avoid breaking
 * users when we change the interface, we made it an abstract class.</p>
 *
 * @param <I> type of input received
 * @param <O> type of intermediate values produced
 */
public abstract class MapOnlyMapper<I, O> extends BaseMapper<I, O, MapOnlyMapperContext<O>> {

  private static final long serialVersionUID = 3407371758265252756L;

  /**
   * Returns a MapOnlyMapper for a given Mapper passing only the values to the output.
   */
  public static <I, Void, V> MapOnlyMapper<I, V> forMapper(Mapper<I, Void, V> mapper) {
    return new MapperAdapter<>(mapper);
  }

  private static class MapperContextAdapter<K, V> implements MapperContext<K, V> {

    private final MapOnlyMapperContext<V> context;

    private MapperContextAdapter(MapOnlyMapperContext<V> context) {
      this.context = context;
    }

    @Override
    public void emit(KeyValue<K, V> value) {
      context.emit(value.getValue());
    }

    @Override
    public int getShardCount() {
      return context.getShardCount();
    }

    @Override
    public int getShardNumber() {
      return context.getShardNumber();
    }

    @Override
    public Counters getCounters() {
      return context.getCounters();
    }

    @Override
    public Counter getCounter(String name) {
      return context.getCounter(name);
    }

    @Override
    public void incrementCounter(String name, long delta) {
      context.incrementCounter(name, delta);
    }

    @Override
    public void incrementCounter(String name) {
      context.incrementCounter(name);
    }

    @Override
    public String getJobId() {
      return context.getJobId();
    }

    @Override
    public void emit(K key, V value) {
      context.emit(value);
    }
  }

  private static class MapperAdapter<I, K, V> extends MapOnlyMapper<I, V> {

    private static final long serialVersionUID = 4786890683072379586L;
    private final Mapper<I, K, V> mapper;

    private MapperAdapter(Mapper<I, K, V> mapper) {
      this.mapper = mapper;
    }

    @Override
    public void setContext(MapOnlyMapperContext<V> context) {
      mapper.setContext(new MapperContextAdapter<K, V>(context));
    }

    @Override
    public long estimateMemoryRequirement() {
      return mapper.estimateMemoryRequirement();
    }

    @Override
    public void beginShard() {
      mapper.beginShard();
    }

    @Override
    public void beginSlice() {
      mapper.beginSlice();
    }

    @Override
    public void endSlice() {
      mapper.endSlice();
    }

    @Override
    public void endShard() {
      mapper.endShard();
    }

    @Override
    public void map(I value) {
      mapper.map(value);
    }
  }
}
