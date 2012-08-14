// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.ReducerInput;

import java.io.Serializable;
import java.util.List;

/**
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <K> type of intermediate keys
 * @param <V> type of intermediate values
 * @param <O> type of output values
 */
public class ShuffleResult<K, V, O> implements Serializable {
  private static final long serialVersionUID = 928463919705565293L;

  private final List<AppEngineFile> reducerInputFiles;
  private final List<? extends OutputWriter<O>> reducerWriters;
  private final List<? extends InputReader<KeyValue<K, ReducerInput<V>>>> reducerReaders;

  public ShuffleResult(List<AppEngineFile> reducerInputFiles,
      List<? extends OutputWriter<O>> reducerWriters,
      List<? extends InputReader<KeyValue<K, ReducerInput<V>>>> reducerReaders) {
    this.reducerInputFiles = checkNotNull(reducerInputFiles, "Null reducerInputFiles");
    this.reducerWriters = checkNotNull(reducerWriters, "Null reducerWriters");
    this.reducerReaders = checkNotNull(reducerReaders, "Null reducerReaders");
  }

  public List<AppEngineFile> getReducerInputFiles() {
    return reducerInputFiles;
  }

  public List<? extends OutputWriter<O>> getReducerWriters() {
    return reducerWriters;
  }

  public List<? extends InputReader<KeyValue<K, ReducerInput<V>>>> getReducerReaders() {
    return reducerReaders;
  }

  @Override public String toString() {
    return getClass().getSimpleName() + "("
        + reducerInputFiles + ", "
        + reducerWriters + ", "
        + reducerReaders
        + ")";
  }

}
