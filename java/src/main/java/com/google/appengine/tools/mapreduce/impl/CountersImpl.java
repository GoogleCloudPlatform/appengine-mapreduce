// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.Counter;
import com.google.appengine.tools.mapreduce.Counters;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

/**
 */
public class CountersImpl implements Counters {

  private static final long serialVersionUID = -8499952345096458550L;

  private final Map<String, Counter> values = new TreeMap<>();

  @Override
  public String toString() {
    StringBuilder out = new StringBuilder(getClass().getSimpleName() + "(");
    Joiner.on(',').appendTo(out, values.values());
    out.append(')');
    return out.toString();
  }

  @Override
  public Counter getCounter(String name) {
    Counter counter = values.get(name);
    if (counter == null) {
      counter = new CounterImpl(name);
      values.put(name, counter);
    }
    return counter;
  }

  @Override
  public Iterable<? extends Counter> getCounters() {
    return Iterables.unmodifiableIterable(values.values());
  }

  @Override
  public void addAll(Counters other) {
    for (Counter c : other.getCounters()) {
      getCounter(c.getName()).increment(c.getValue());
    }
  }

  private static class CounterImpl implements Counter, Serializable {
    private static final long serialVersionUID = 5872696485441192885L;

    private final String name;
    private long value;

    CounterImpl(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name + "=" + value;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public long getValue() {
      return value;
    }

    @Override
    public void increment(long delta) {
      value += delta;
    }
  }
}
