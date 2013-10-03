package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.ReducerInput;
import com.google.appengine.tools.mapreduce.impl.sort.LexicographicalComparator;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.appengine.tools.mapreduce.inputs.PeekingInputReader;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * A reader class that performs merging of multiple different inputs.
 * This is done to combine the contents of multiple sorted files while preserving the sort order.
 * In order to do this it operates on a peeking input reader that returns KeyValue pairs.
 * It uses the lexicographicalComparator to compare the serialized keys and returns the lowest one
 * first.
 * 
 * @param <K> The type of the key to be returned in the key Value pair.
 * @param <V> The type of the value
 */
final class MergingReader<K, V> extends InputReader<KeyValue<K, Iterator<V>>> {

  private static final long serialVersionUID = 4731927175388671578L;
  private static final LexicographicalComparator comparator = new LexicographicalComparator();
  private List<PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>>> readers;
  private Marshaller<K> keyMarshaller;
  private transient ByteBuffer lastKey;
  private transient PriorityQueue<PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>>>
      lowestReaderQueue;

  MergingReader(List<PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>>> readers,
      Marshaller<K> keyMarshaller) {
    this.readers = Preconditions.checkNotNull(readers);
    this.keyMarshaller = Preconditions.checkNotNull(keyMarshaller);
  }
  
  /**
   * Called by java serialization
   */
  private void readObject(ObjectInputStream aInputStream) throws ClassNotFoundException,
      IOException {
    aInputStream.defaultReadObject();
    lastKey = SerializationUtil.readObjectFromObjectStreamUsingMarshaller(
        Marshallers.getByteBufferMarshaller(), aInputStream);
  }

  /**
   * Called by java serialization
   */
  private void writeObject(ObjectOutputStream aOutputStream) throws IOException {
    aOutputStream.defaultWriteObject();
    SerializationUtil.writeObjectToOutputStreamUsingMarshaller(lastKey,
        Marshallers.getByteBufferMarshaller(), aOutputStream);
  }
  
  @Override
  public void beginSlice() throws IOException {
    Comparator<PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>>> nextReaderComparator =
        new Comparator<PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>>>() {
          @Override
          public int compare(PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>> r1,
              PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>> r2) {
            return comparator.compare(r1.peek().getKey(), r2.peek().getKey());
          }
        };
    lowestReaderQueue = new PriorityQueue<PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>>>(
        Math.max(1, readers.size()), nextReaderComparator);
    for (PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>> reader : readers) {
      reader.beginSlice();
      addReaderToQueueIfNotEmpty(reader);
    }
  }

  @Override
  public void endSlice() throws IOException {
    for (PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>> reader : readers) {
      reader.endSlice();
    }
  }

  /**
   * A reader that combines consecutive values with the same key into one ReducerInput.
   */
  private class CombiningReader extends ReducerInput<V> {
    private ByteBuffer key;
    private Iterator<V> currentValues;

    CombiningReader(ByteBuffer key, Iterator<V> currentValues) {
      this.key = Preconditions.checkNotNull(key);
      this.currentValues = Preconditions.checkNotNull(currentValues);
    }

    /**
     * @return true iff there are more values associated with this key.
     */
    @Override
    public boolean hasNext() {
      if (currentValues.hasNext()) {
        return true;
      }
      if (lowestReaderQueue.isEmpty()) {
        return false;
      }
      return comparator.compare(lowestReaderQueue.peek().peek().getKey(), key) == 0;
    }
    
    /**
     * @throws NoSuchElementException if there are no more values for this key.
     */
    @Override
    public V next() {
      if (currentValues.hasNext()) {
        return currentValues.next();
      }
      KeyValue<ByteBuffer, Iterator<V>> keyValue =
          (lowestReaderQueue.isEmpty()) ? null : lowestReaderQueue.peek().peek();
      if (keyValue == null || comparator.compare(keyValue.getKey(), key) != 0) {
        throw new NoSuchElementException();
      }
      consumePeekedValue(keyValue);
      currentValues = keyValue.getValue();
      return next();
    }
    
    /**
     * Helper to consume the value that was just peeked.
     */
    private void consumePeekedValue(KeyValue<ByteBuffer, Iterator<V>> peekedKeyValue) {
      PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>> reader = lowestReaderQueue.poll();
      consumePeekedValueFromReader(peekedKeyValue, reader);
      addReaderToQueueIfNotEmpty(reader);
    }
  }
  
  /**
   * @param peekedKeyValue The peeked value to be consumed
   * @param reader Where the value came from
   */
  private void consumePeekedValueFromReader(KeyValue<ByteBuffer, Iterator<V>> peekedKeyValue,
      PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>> reader) {
    if (reader == null || !peekedKeyValue.equals(reader.next())) {
      throw new ConcurrentModificationException("Reading from values is not threadsafe.");
    }
  }
  
  /**
   * Returns the next KeyValues object for the reducer. This is the entry point.
   * @throws NoSuchElementException if there are no more keys in the input.
   */
  @Override
  public KeyValue<K, Iterator<V>> next() throws IOException, NoSuchElementException {
    skipLeftoverItems();
    PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>> reader = lowestReaderQueue.remove();
    KeyValue<ByteBuffer, Iterator<V>> lowest = reader.next();
    CombiningReader values = new CombiningReader(lowest.getKey(), lowest.getValue());
    addReaderToQueueIfNotEmpty(reader);
    lastKey = lowest.getKey();
    return new KeyValue<K, Iterator<V>>(keyMarshaller.fromBytes(lastKey.slice()), values);
  }

  private void addReaderToQueueIfNotEmpty(
      PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>> reader) {
    if (reader.hasNext()) {
      lowestReaderQueue.add(reader);
    }
  }

  /**
   * It is possible that a reducer does not iterate over all of the items given to it for a given
   * key. However on the next callback they expect to receive items for the following key. this
   * method skips over all the left over items from the previous key they did not read.
   */
  private void skipLeftoverItems() {
    if (lastKey == null || lowestReaderQueue.isEmpty()) {
      return;
    }
    boolean itemSkiped;
    do {
      PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>> reader = lowestReaderQueue.remove();
      itemSkiped = skipItemsOnReader(reader);
      addReaderToQueueIfNotEmpty(reader);
    } while (itemSkiped);
  }

  /**
   * Helper function for skipLeftoverItems to skip items matching last key on a single reader.
   */
  private boolean skipItemsOnReader(PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>> reader) {
    boolean itemSkipped = false;
    KeyValue<ByteBuffer, Iterator<V>> keyValue = reader.peek();
    while (keyValue != null && (comparator.compare(keyValue.getKey(), lastKey) == 0)) {
      consumePeekedValueFromReader(keyValue, reader);
      itemSkipped = true;
      keyValue = reader.peek();
    }
    return itemSkipped;
  }

  @Override
  public Double getProgress() {
    double total = 0;
    for (PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>> reader : readers) {
      Double progress = reader.getProgress();
      if (progress != null) {
        total += progress;
      }
    }
    return total / readers.size();
  }

}