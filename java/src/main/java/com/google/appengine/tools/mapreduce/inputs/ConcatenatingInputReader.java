package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.NoSuchElementException;

/**
 * Reads multiple inputs in sequence. The caller of the constructor is expected to provide an
 * interface for getting the next underlying reader.
 *
 * @param <I> type of values produced by this input
 */
public final class ConcatenatingInputReader<I> extends InputReader<I> {

  /**
   * This should initialize and return readers as needed until there are no more to be created and
   * then return null. There is an additional method estimateTotalNumberOfReaders. This is used for
   * reporting progress (NOT determining when there is no more to read) so an estimate is
   * acceptable, even if it changes over time.
   *
   * @param <I> type of values produced by this input
   */
  public static interface ReaderCreator<I> extends Serializable {
    /**
     * @return a new reader who's input should follow the current one's or null if there is nothing
     *         else to read.
     */
    InputReader<I> createNextReader();

    /**
     * @return The number of readers that will ultimately be returned from createNextReader. Used to
     *         report progress to the user only. Not required to be accurate and can change over
     *         time.
     */
    int estimateTotalNumberOfReaders();
  }

  private static final long serialVersionUID = 8138313792847251310L;
  private int index;
  private InputReader<I> reader;
  private ConcatenatingInputReader.ReaderCreator<I> creator;

  public ConcatenatingInputReader(ConcatenatingInputReader.ReaderCreator<I> creator) {
    this.creator = Preconditions.checkNotNull(creator);
    this.index = -1;
    this.reader = null;
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
        reader = creator.createNextReader();
        if (reader == null) {
          throw new NoSuchElementException();
        }
        reader.beginSlice();
        index++;
      }
      try {
        return reader.next();
      } catch (NoSuchElementException e) {
        reader.endSlice();
        reader = null;
      }
    }
  }

  @Override
  public Double getProgress() {
    if (index < 0) {
      return 0.0;
    }
    double progressInCurrentFile = 0.5;
    if (reader != null) {
      Double progress = reader.getProgress();
      if (progress != null) {
        progressInCurrentFile = progress;
      }
    }
    return Math.min(1.0, (progressInCurrentFile + index) / creator.estimateTotalNumberOfReaders());
  }

}
