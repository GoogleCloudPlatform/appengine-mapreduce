// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.ReducerInput;

import java.util.Iterator;

/**
 * Utilities related to {@link ReducerInput}.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class ReducerInputs {

  private ReducerInputs() {}

  private static class IteratorReducerInput<V> extends ReducerInput<V> {
    private final Iterator<V> i;

    public IteratorReducerInput(Iterator<V> i) {
      this.i = i;
    }

    @Override public boolean hasNext() {
      return i.hasNext();
    }

    @Override public V next() {
      return i.next();
    }

    @Override public String toString() {
      return "ReducerInputs.fromIterator(" + i + ")";
    }
  }

  public static <V> ReducerInput<V> fromIterator(Iterator<V> i) {
    return new IteratorReducerInput<V>(i);
  }

  public static <V> ReducerInput<V> fromIterable(final Iterable<V> x) {
    return new IteratorReducerInput<V>(x.iterator()) {
      @Override public String toString() {
        return "ReducerInputs.fromIterable(" + x + ")";
      }
    };
  }

}
