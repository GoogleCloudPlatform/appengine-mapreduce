package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTask;

/**
 * A simple extension of {@link IncrementalTask} to add information for display in a UI.
 */
public interface IncrementalTaskWithContext extends IncrementalTask {

  IncrementalTaskContext getContext();
}
