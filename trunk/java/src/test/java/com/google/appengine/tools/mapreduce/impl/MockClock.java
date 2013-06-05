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

package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.impl.util.Clock;
import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.List;

/**
 * Mock clock that returns a predefined sequence of times.
 *
 * Package visible because it's a utility class for mapreduce.
 *
 *
 */
public class MockClock implements Clock {
// ------------------------------ FIELDS ------------------------------

  private List<Long> times;
  private int currentOffset = 0;
  private boolean repeatLastTime;

// --------------------------- CONSTRUCTORS ---------------------------

  /**
   * Makes the clock return the given time repeatedly.
   */
  public MockClock(long time) {
    times = Arrays.asList(new Long(time));
    repeatLastTime = true;
  }

  /**
   * Initializes a clock that returns a sequence of predefined times.
   *
   * @param times the sequence of times to return
   * @param repeatLastTime defines the behavior if more than times.sizes() calls
   * to {@link #currentTimeMillis()} are made. If true, then the last time
   * is repeatedly returned. Otherwise, an exception is thrown.
   */
  public MockClock(List<Long> times, boolean repeatLastTime) {
    this.times = times;
    this.repeatLastTime = repeatLastTime;
  }

// ------------------------ INTERFACE METHODS ------------------------


// --------------------- Interface Clock ---------------------

  /**
   * Returns the next time in the sequence.
   */
  @Override
  public long currentTimeMillis() {
    Preconditions.checkNotNull(times);
    Preconditions.checkState(times.size() > 0);
    Preconditions.checkElementIndex(currentOffset, times.size());

    long timeToReturn = times.get(currentOffset).longValue();

    if (!repeatLastTime || currentOffset < times.size() - 1) {
      currentOffset++;
    }

    return timeToReturn;
  }
}
