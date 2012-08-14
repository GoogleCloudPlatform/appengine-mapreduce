// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Input is the data source specification for the job. Input simply defines data source, while
 * {@link InputReader} handles reading itself.
 *
 * <p>This class is really an interface that might be evolving. In order to avoid breaking
 * users when we change the interface, we made it an abstract class.</p>
 *
 *
 * @param <I> type of values produced by this input
 */
public abstract class Input<I> implements Serializable {
  private static final long serialVersionUID = 8796820298129705263L;

  /**
   * Returns a list of readers for this input.  It is the {@code Input}'s
   * responsibility to determine an appropriate number of readers to split into.
   * This could be specified by the user or determined algorithmically.
   *
   * The number of input readers returned determines the number of map shards.
   */
  public abstract List<? extends InputReader<I>> createReaders() throws IOException;

}
