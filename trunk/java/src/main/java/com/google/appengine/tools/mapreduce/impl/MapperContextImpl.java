// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.MapperContext;
import com.google.appengine.tools.mapreduce.OutputWriter;

/**
 * @author ohler@google.com (Christian Ohler)
 */
class MapperContextImpl<K, V> extends BaseShardContext<KeyValue<K, V>>
    implements MapperContext<K, V> {

  MapperContextImpl(IncrementalTaskContext c, OutputWriter<KeyValue<K, V>> output) {
    super(c, output);
  }

  @Override
  public void emit(K key, V value) {
    emit(KeyValue.of(key, value));
  }
}
