// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import java.util.Iterator;

/**
 * Enumerates the reducer's input values for a given key.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <V> type of values provided by this input
 */
// Not serializable; since reduce() receives an iterator, we will never end a
// slice while an iterator is active.
public abstract class ReducerInput<V> implements Iterator<V> {

  @Override public void remove() {
    throw new UnsupportedOperationException("Can't remove() on ReducerInput: " + this);
  }

}
