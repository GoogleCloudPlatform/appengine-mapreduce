// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.MapperContext;
import com.google.appengine.tools.mapreduce.OutputWriter;

import java.io.IOException;

/**
 * @author ohler@google.com (Christian Ohler)
 */
class MapperContextImpl<K, V> extends MapperContext<K, V> {

  private final OutputWriter<KeyValue<K, V>> output;

  MapperContextImpl(String mrJobId, OutputWriter<KeyValue<K, V>> output, int shardNumber,
      Counters counters) {
    super(mrJobId, shardNumber, counters);
    this.output = checkNotNull(output, "Null output");
  }

  @Override public void emit(K key, V value) {
    try {
      output.write(KeyValue.of(key, value));
    } catch (IOException e) {
      throw new RuntimeException(output + ".write(" + key + ", " + value + ") threw IOException",
          e);
    }
  }

}
