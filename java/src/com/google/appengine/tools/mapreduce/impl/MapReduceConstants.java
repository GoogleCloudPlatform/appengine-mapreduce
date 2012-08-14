// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

/**
 * @author ohler@google.com (Christian Ohler)
 */
public class MapReduceConstants {

  private MapReduceConstants() {}

  public static final String MAP_OUTPUT_MIME_TYPE =
      "application/vnd.appengine.mapreduce.map-output.records";

  public static final String REDUCE_INPUT_MIME_TYPE =
      "application/vnd.appengine.mapreduce.reduce-input.records";

}
