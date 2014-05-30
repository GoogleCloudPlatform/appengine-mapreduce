// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.outputs.NoOutput;

/**
 * Specification for a map-only job.  The values here affect what computation
 * is performed in the Map job and its side-effects, but not how it is
 * executed; see {@link MapSettings} for that.
 *
 * @param <I> type of input values
 * @param <O> type of output values
 * @param <R> type of result returned by the {@link Output}
 */
public final class MapSpecification<I, O, R> extends BaseSpecification<I, O, R> {

  private static final long serialVersionUID = 1422503827577339560L;

  public static class Builder<I, O, R> extends
      BaseSpecification.Builder<Builder<I, O, R>, I, O, R> {

    private MapOnlyMapper<? extends I, O> mapper;

    public Builder() {
      jobName = "MapJob";
    }

    /**
     * A builder for map job using {@link NoOutput} as output.
     */
    @SuppressWarnings("unchecked")
    public Builder(Input<I> input, MapOnlyMapper<I, Void> mapper) {
      this(input, (MapOnlyMapper<I, O>) mapper, (Output<O, R>) new NoOutput<Void, Void>());
    }

    public Builder(Input<I> input, MapOnlyMapper<I, O> mapper, Output<O, R> output) {
      this();
      this.input = input;
      this.mapper = mapper;
      this.output = output;
    }

    @Override
    protected Builder<I, O, R> self() {
      return this;
    }

    /**
     * @param mapper processes the input and optionally generates output value
     */
    public Builder<I, O, R> setMapper(MapOnlyMapper<? extends I, O> mapper) {
      this.mapper = mapper;
      return this;
    }

    /**
     * A convenience method that wraps the given {@code mapper} using
     * {@link MapOnlyMapper#forMapper(Mapper)}.
     */
    public Builder<I, O, R> setMapper(Mapper<? extends I, Void, O> mapper) {
      this.mapper = MapOnlyMapper.forMapper(mapper);
      return this;
    }

    public MapSpecification<I, O, R> build() {
      return new MapSpecification<>(this);
    }
  }

  private final MapOnlyMapper<I, O> mapper;

  @SuppressWarnings("rawtypes")
  private MapSpecification(Builder builder) {
    super(builder);
    mapper = checkNotNull(builder.mapper, "Null mapper");
  }

  MapOnlyMapper<I, O> getMapper() {
    return mapper;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + getJobName() + ", " + getInput() + ", "
        + mapper + ", " + getOutput() + ")";
  }
}
