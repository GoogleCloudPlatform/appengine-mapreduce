// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.demos.mapreduce.entitycount;

import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Reducer;
import com.google.appengine.tools.mapreduce.ReducerInput;

import java.util.logging.Logger;

/**
 * Aggregates counts.  The key identifies the counter, the output value is the
 * sum of all input values for the given key.
 *
 * @author ohler@google.com (Christian Ohler)
 */
class CountReducer extends Reducer<String, Long, KeyValue<String, Long>> {
  private static final long serialVersionUID = 1316637485625852869L;

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(CountReducer.class.getName());

  public CountReducer() {
  }

  private void emit(String key, long outValue) {
    //log.info("emit(" + outValue + ")");
    getContext().emit(KeyValue.of(key, outValue));
  }

  @Override public void beginShard() {
  }

  @Override public void beginSlice() {
  }

  @Override public void reduce(String key, ReducerInput<Long> values) {
    //log.info("reduce(" + key + ", " + values + ")");
    long total = 0;
    while (values.hasNext()) {
      long value = values.next();
      total += value;
    }
    emit(key, total);
  }

  @Override public void endShard() {
  }

  @Override public void endSlice() {
  }

}
