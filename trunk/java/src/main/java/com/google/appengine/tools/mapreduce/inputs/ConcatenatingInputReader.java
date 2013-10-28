package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Reads multiple inputs in sequence. The caller of the constructor is expected to provide an
 * interface for getting the next underlying reader.
 *
 * @param <I> type of values produced by this input
 */
public final class ConcatenatingInputReader<I> extends InputReader<I> {

  private static final long serialVersionUID = 8138313792847251310L;
  private int index;
  private InputReader<I> reader;
  private final List<? extends InputReader<I>> readers;

  public ConcatenatingInputReader(List<? extends InputReader<I>> readers) {
    this.readers = Preconditions.checkNotNull(readers);
    this.index = 0;
  }

  @Override
  public void beginSlice() throws IOException {
    if (reader != null) {
      reader.beginSlice();
    }
  }

  @Override
  public void endSlice() throws IOException {
    if (reader != null) {
      reader.endSlice();
    }
  }

  @Override
  public I next() throws IOException, NoSuchElementException {
    while (true) {
      if (reader == null) {
        if (index >= readers.size()) {
          throw new NoSuchElementException();
        }
        reader = readers.get(index++);
        reader.open();
        reader.beginSlice();
      }
      try {
        return reader.next();
      } catch (NoSuchElementException e) {
        reader.endSlice();
        reader.close();
        reader = null;
      }
    }
  }

  @Override
  public Double getProgress() {
    if (index < 0) {
      return 0.0;
    }
    if (index >= readers.size()) {
      return 1.0;
    }
    double progressInCurrentFile;
    if (reader == null) {
      progressInCurrentFile = 0.0;
    } else {
      Double progress = reader.getProgress();
      if (progress == null) {
        progressInCurrentFile = 0.5;
      } else {
        progressInCurrentFile = progress;
      }
    }
    return (progressInCurrentFile + index) / readers.size();
  }

  @Override
  public void open() throws IOException {
    reader = null;
    index = 0;
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

  @Override
  public long estimateMemoryRequirment() {
    long max = 0;
    for (InputReader<I> reader : readers) {
      max = Math.max(max, reader.estimateMemoryRequirment());
    }
    return max;
  }

}
