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

package com.google.appengine.tools.mapreduce.mappers;

import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Mapper;

/**
 * A mapper that passes an incoming KeyValue to it's output.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class IdentityMapper<K, V> extends Mapper<KeyValue<K, V>, K, V> {

  private static final long serialVersionUID = -8243493999040989299L;

  @Override
  public void map(KeyValue<K, V> input) {
    emit(input.getKey(), input.getValue());
  }
}
