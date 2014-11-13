// Copyright 2014 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.appengine.tools.mapreduce.reducers;

import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Reducer;
import com.google.appengine.tools.mapreduce.ReducerInput;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * A passthrough reducer that passes it's input to it's output.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class IdentityReducer<K, V> extends Reducer<K, V, KeyValue<K, List<V>>> {

  private static final long serialVersionUID = -8226312047392268923L;

  private final int maxGroupedValues;

  /**
   * @param maxGroupedValues The number of consecutive values that will be grouped together in a
   *        single emit call. If there are more than this number of values emit will be called
   *        multiple times with the same key.
   */
  public IdentityReducer(int maxGroupedValues) {
    Preconditions.checkArgument(maxGroupedValues > 0);
    this.maxGroupedValues = maxGroupedValues;
  }

  @Override
  public void reduce(K key, ReducerInput<V> values) {
    List<V> pendingValues = new ArrayList<>();
    while (values.hasNext()) {
      pendingValues.add(values.next());
      if (pendingValues.size() >= maxGroupedValues) {
        emit(new KeyValue<>(key, pendingValues));
        // Reallocate the list because emit() may not immediately consume the iterator passed to it
        pendingValues = new ArrayList<>();
      }
    }
    if (!pendingValues.isEmpty()) {
      emit(new KeyValue<>(key, pendingValues));
    }
  }
}
