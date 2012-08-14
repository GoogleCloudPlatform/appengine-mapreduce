/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.mapreduce.impl.util;

/**
 * Simple mockable clock.
 *
 * Package visible because it's a utility class for MapReduce.
 *
 */
public interface Clock {
// -------------------------- INSTANCE METHODS --------------------------

  /**
   * Returns the current time as defined by the particular clock implementation
   * in milliseconds from the epoch.
   */
  long currentTimeMillis();
}
