package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

public class InMemoryOutputWriter<O> extends OutputWriter<O> {

  private static final long serialVersionUID = 528522943983621278L;

  private boolean closed = false;
  private final List<O> accu = Lists.newArrayList();
  private transient List<O> slice;

  @Override
  public String toString() {
    return getClass().getName() + "(" + accu.size() + " items" + (closed ? ", closed" : " so far")
        + ")";
  }

  @Override
  public void beginShard() {
    closed = false;
    accu.clear();
  }

  @Override
  public void beginSlice() {
    slice = Lists.newArrayList();
  }

  @Override
  public void write(O value) {
    Preconditions.checkState(!closed, "%s: Already closed", this);
    slice.add(value);
  }

  @Override
  public void endSlice() {
    accu.addAll(slice);
    slice = null;
  }

  @Override
  public void endShard() {
    closed = true;
  }

  @Override
  public boolean allowSliceRetry() {
    return true;
  }

  public List<O> getResult() {
    return accu;
  }
}