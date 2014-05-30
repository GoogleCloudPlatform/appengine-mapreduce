// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.MapOnlyMapperContext;
import com.google.appengine.tools.mapreduce.OutputWriter;

/**
 */
class MapOnlyMapperContextImpl<O> extends BaseShardContext<O> implements MapOnlyMapperContext<O> {

  MapOnlyMapperContextImpl(IncrementalTaskContext c, OutputWriter<O> output) {
    super(c, output);
  }
}
