// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;


/**
 * Context for {@link MapOnlyMapper} execution.
 *
 * @param <O> type of output values produced by the mapper
 */
public interface MapOnlyMapperContext<O> extends WorkerContext<O> {
}
