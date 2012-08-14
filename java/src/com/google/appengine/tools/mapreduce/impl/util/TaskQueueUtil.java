// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.impl.util;

/**
 */
public class TaskQueueUtil {
// --------------------------- CONSTRUCTORS ---------------------------

  private TaskQueueUtil() {
  }

// -------------------------- STATIC METHODS --------------------------

  public static String formatTaskName(String format, Object...args) {
    return normalizeTaskName(String.format(format, args));
  }

  public static String normalizeTaskName(String taskName) {
    return taskName.replaceAll("[^0-9a-zA-Z-_]", "_");
  }
}
