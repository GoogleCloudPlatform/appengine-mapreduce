// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.MapperContext;
import com.google.appengine.tools.mapreduce.OutputWriter;

import java.io.IOException;

/**
 * @author ohler@google.com (Christian Ohler)
 */
class MapperContextImpl<K, V> extends BaseShardContext implements MapperContext<K, V> {

  private final OutputWriter<KeyValue<K, V>> output;

  MapperContextImpl(IncrementalTaskContext c, OutputWriter<KeyValue<K, V>> output) {
    super(c);
    this.output = checkNotNull(output, "Null output");
  }

  @Override
  public void emit(K key, V value) {
    emit(KeyValue.of(key, value));
  }

  @Override
  public void emit(KeyValue<K, V> value) {
    try {
      output.write(value);
    } catch (IOException e) {
      throw new RuntimeException(
          output + ".write(" + value + ") threw IOException", e);
    }
  }
}
