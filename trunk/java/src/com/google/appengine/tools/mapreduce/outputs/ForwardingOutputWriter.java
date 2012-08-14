// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.OutputWriter;

import java.io.IOException;

/**
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <O> type of values accepted by this output
 */
abstract class ForwardingOutputWriter<O> extends OutputWriter<O> {
  private static final long serialVersionUID = 738487653896786084L;

  protected abstract OutputWriter<?> getDelegate();

  @Override public void beginSlice() throws IOException {
    getDelegate().beginSlice();
  }

  public void endSlice() throws IOException {
    getDelegate().endSlice();
  };

  public void close() throws IOException {
    getDelegate().close();
  }

}
