package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.impl.util.SerializableValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An {@link InputReader} that allows peeking at records without consuming them.
 *
 * @param <T> type of values produced by this input
 */
public class PeekingInputReader<T> extends UnmarshallingInputReader<T> implements Iterator<T> {

  private static final long serialVersionUID = -1740668578124485473L;
  private SerializableValue<T> peekedItem; // assigned in readObject & peek

  public PeekingInputReader(InputReader<ByteBuffer> reader, Marshaller<T> marshaller) {
    super(reader, marshaller);
  }

  /**
   * The next element in the input. (Which may or may not have been peeked)
   *
   * @throws RuntimeException In the event that reading from input fails.
   * @throws NoSuchElementException if there are no more elements in the input.
   */
  @Override
  public T next() throws NoSuchElementException {
    T result;
    if (peekedItem != null) {
      result = peekedItem.getValue();
      peekedItem = null;
    } else {
      try {
        result = super.next();
      } catch (IOException e) {
        throw new RuntimeException("Failed to read next input", e);
      }
    }
    return result;
  }

  /**
   * Returns the element that will be returned by {@link #next()} next. Or null if there is no next
   * element. If null elements are allowed in the input, one can distinguish between the end of the
   * input and a null element by calling {@link #hasNext()}
   *
   * @throws RuntimeException In the event that reading from input fails.
   */
  public T peek() {
    if (peekedItem == null) {
      try {
        peekedItem = SerializableValue.of(getMarshaller(), super.next());
      } catch (NoSuchElementException e) {
        // ignore.
      } catch (IOException e) {
        throw new RuntimeException("Failed to read next input", e);
      }
    }
    return peekedItem == null ? null : peekedItem.getValue();
  }

  @Override
  public boolean hasNext() {
    peek();
    return peekedItem != null;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
