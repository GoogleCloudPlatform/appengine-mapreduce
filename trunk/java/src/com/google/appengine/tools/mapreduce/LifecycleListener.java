// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import java.io.Serializable;

/**
 * Allows tracking slice lifecycle.
 *
 * <p>Each slice starts with a call {@link #beginSlice}, then performs the
 * actual work, and ends with a call to {@link #endSlice}.  Between two slices,
 * before the first slice, or after the final slice, the listener may go through
 * serialization and deserialization.
 *
 * <p>If a slice is aborted, there is no guarantee whether {@link #endSlice}
 * will be called; however, if it is not called, the listener will not be
 * serialized or used further in any way.  If the slice is retried later, the
 * listener serialized after the previous slice will be deserialized
 * again.
 *
 * <p>This class is really an interface that might be evolving.  In order to
 * avoid breaking users when we change the interface, we made it an abstract
 * class.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public abstract class LifecycleListener implements Serializable {
  private static final long serialVersionUID = 969557893999706161L;

  // TODO(ohler): Should we add beginShard and/or endShard?

  /**
   * Notifies the listener that a new slice is about to begin processing, after
   * possibly having gone through serialization and deserialization.
   */
  // TODO(ohler): Should we pass in the WorkerContext?  Could be useful if some
  // listener wants to publish counters.
  public void beginSlice() {}

  /**
   * Notifies the listener that there is no more data to process in the current
   * slice, and prepares it for possible serialization.
   */
  public void endSlice() {}

}
