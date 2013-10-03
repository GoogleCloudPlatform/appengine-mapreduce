package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.InputReader;

import java.io.IOException;
import java.util.NoSuchElementException;


/**
 * An {@link InputReader} delegates to another implementation.
 *
 * @param <T> type of values returned by this reader
 */
public abstract class ForwardingInputReader<T> extends InputReader<T> {

  private static final long serialVersionUID = 443622749959231115L;

  protected abstract InputReader<T> getDelegate();

  @Override
  public T next() throws IOException, NoSuchElementException {
    return getDelegate().next();
  }

  @Override
  public Double getProgress() {
    return getDelegate().getProgress();
  }

  @Override
  public void beginSlice() throws IOException {
    getDelegate().beginSlice();
  }

  @Override
  public void endSlice() throws IOException {
    getDelegate().endSlice();
  }

  @Override
  public void open() throws IOException {
    getDelegate().open();
  }

  @Override
  public void close() throws IOException {
    getDelegate().close();
  }

}
