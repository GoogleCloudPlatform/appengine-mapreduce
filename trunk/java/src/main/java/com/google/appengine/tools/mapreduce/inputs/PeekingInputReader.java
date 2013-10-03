package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
  // TODO(user): May be a problem if peeked values are large b/10294530
  private transient T peekedItem = null;
  private boolean peeked = false;

  public PeekingInputReader(InputReader<ByteBuffer> reader, Marshaller<T> marshaller){
    super(reader, marshaller);
  }

  /**
   * Called by java serialization
   * Reads the peeked value. (Uses the user provided marshaller because the object may not be
   * java serializable.)
   */
  private void readObject(ObjectInputStream aInputStream) throws ClassNotFoundException,
      IOException {
    aInputStream.defaultReadObject();
    peekedItem =
        SerializationUtil.readObjectFromObjectStreamUsingMarshaller(marshaller, aInputStream);
  }

  /**
   * Called by java serialization Writes the peeked value. (Uses the user provided marshaller
   * because the object may not be java serializable.)
   */
  private void writeObject(ObjectOutputStream aOutputStream) throws IOException {
    aOutputStream.defaultWriteObject();
    SerializationUtil.writeObjectToOutputStreamUsingMarshaller(peekedItem, marshaller,
        aOutputStream);
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
    if (peeked) {
      result = peekedItem;
    } else {
      try {
        result = super.next();
      } catch (IOException e) {
        throw new RuntimeException("Failed to read next input", e);
      }
    }
    peekedItem = null;
    peeked = false;
    return result;
  }

  /**
   * @return the element that will be returned by next() next. Or null if there is no next element.
   * @throws RuntimeException In the event that reading from input fails.
   */
  public T peek() {
    if (!peeked) {
      try {
        peekedItem = super.next();
        peeked = true;
      } catch (NoSuchElementException e) {
        // ignore.
      } catch (IOException e) {
        throw new RuntimeException("Failed to read next input", e);
      }
    }
    return peekedItem;
  }

  @Override
  public boolean hasNext() {
    peek();
    return peeked;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
