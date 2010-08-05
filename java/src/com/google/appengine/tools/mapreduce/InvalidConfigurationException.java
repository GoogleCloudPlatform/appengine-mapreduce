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

package com.google.appengine.tools.mapreduce;

/**
 * Exception thrown if the Configuration specified in the start handler
 * is invalid.
 * 
 * @author frew@google.com (Fred Wulff)
 *
 */
public class InvalidConfigurationException extends RuntimeException {
  public InvalidConfigurationException(String s) {
    super(s);
  }
  
  public InvalidConfigurationException(String s, Exception e) {
    super(s, e);
  }
}
