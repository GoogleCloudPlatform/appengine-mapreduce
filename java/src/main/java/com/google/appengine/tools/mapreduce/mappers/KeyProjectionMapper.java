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

import com.google.appengine.tools.mapreduce.Mapper;

/**
 * A pass through mapper that passes the input to the output key.
 *
 * @param <T> type input that is passed on as the key
 */
public class KeyProjectionMapper<T> extends Mapper<T, T, Void> {

  private static final long serialVersionUID = -3998292521173820259L;

  @Override
  public void map(T value) {
    emit(value, null);
  }
}
