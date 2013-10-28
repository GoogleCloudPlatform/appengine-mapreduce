// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Keeps track of listeners to be notified of events related to the worker
 * lifecycle; see {@link LifecycleListener}.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class LifecycleListenerRegistry implements Serializable {
  private static final long serialVersionUID = 992608587128906678L;

  public final Set<LifecycleListener> listeners = new LinkedHashSet<>();

  // Private constructor to disallow subclasses (more flexible than making the class final).
  private LifecycleListenerRegistry() {
  }

  public static LifecycleListenerRegistry create() {
    return new LifecycleListenerRegistry();
  }

  /**
   * Adds a listener to be notified on worker lifecycle events.
   */
  public void addListener(LifecycleListener l) {
    Preconditions.checkState(!listeners.contains(l), "%s: Listener already registered: %s",
        this, l);
    listeners.add(l);
  }

  /**
   * Removes a listener.
   */
  public void removeListener(LifecycleListener l) {
    Preconditions.checkState(listeners.contains(l), "%s: Listener not registered: %s",
        this, l);
    listeners.remove(l);
  }

  /**
   * Returns the listeners.  It is undefined whether the returned list is a live
   * view or a copy, but iterating over it and calling {@link #removeListener}
   * on the current element is permitted.  Do not attempt to modify the returned
   * list directly.
   */
  public List<LifecycleListener> getListeners() {
    return ImmutableList.copyOf(listeners);
  }

}
