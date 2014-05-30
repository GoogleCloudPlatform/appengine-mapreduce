package com.google.appengine.tools.mapreduce.impl;

import static com.google.appengine.tools.mapreduce.Marshallers.getByteBufferMarshaller;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.ReducerInput;
import com.google.appengine.tools.mapreduce.impl.sort.LexicographicalComparator;
import com.google.appengine.tools.mapreduce.impl.util.SerializableValue;
import com.google.appengine.tools.mapreduce.inputs.PeekingInputReader;
import com.google.common.base.Preconditions;

import java.io.IOException;
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
  private final List<PeekingInputReader<KeyValue<ByteBuffer, ? extends Iterable<V>>>> readers;
  private final Marshaller<K> keyMarshaller;
  private SerializableValue<ByteBuffer> lastKey;
  private transient PriorityQueue<PeekingInputReader<KeyValue<ByteBuffer, ? extends Iterable<V>>>>
      lowestReaderQueue;

  MergingReader(List<PeekingInputReader<KeyValue<ByteBuffer, ? extends Iterable<V>>>> readers,
      Marshaller<K> keyMarshaller) {
    this.readers = Preconditions.checkNotNull(readers);
    this.keyMarshaller = Preconditions.checkNotNull(keyMarshaller);
  }

  @Override
  public void beginShard() throws IOException {
    for (PeekingInputReader<?> reader : readers) {
      reader.beginShard();
    }
  }

  @Override
  public void beginSlice() throws IOException {
    Comparator<PeekingInputReader<KeyValue<ByteBuffer, ? extends Iterable<V>>>> pqComparator =
        new Comparator<PeekingInputReader<KeyValue<ByteBuffer, ? extends Iterable<V>>>>() {
          @Override
          public int compare(PeekingInputReader<KeyValue<ByteBuffer, ? extends Iterable<V>>> r1,
              PeekingInputReader<KeyValue<ByteBuffer, ? extends Iterable<V>>> r2) {
            return comparator.compare(r1.peek().getKey(), r2.peek().getKey());
          }
        };
    lowestReaderQueue = new PriorityQueue<>(Math.max(1, readers.size()), pqComparator);
    for (PeekingInputReader<KeyValue<ByteBuffer, ? extends Iterable<V>>> reader : readers) {
      reader.beginSlice();
      addReaderToQueueIfNotEmpty(reader);
    }
  }

  @Override
  public void endSlice() throws IOException {
    for (PeekingInputReader<?> reader : readers) {
      reader.endSlice();
    }
  }

  /**
   * A reader that combines consecutive values with the same key into one ReducerInput.
   */
  private class CombiningReader extends ReducerInput<V> {
    private final ByteBuffer key;
    private Iterator<V> currentValues;

    CombiningReader(ByteBuffer key, Iterable<V> currentValues) {
      this.key = Preconditions.checkNotNull(key);
      Preconditions.checkNotNull(currentValues);
      this.currentValues = currentValues.iterator();
    }

    /**
     * @return true iff there are more values associated with this key.
     */
    @Override
    public boolean hasNext() {
      if (currentValues.hasNext()) {
        return true;
      }
      while (!lowestReaderQueue.isEmpty()) {
        PeekingInputReader<KeyValue<ByteBuffer, ? extends Iterable<V>>> reader =
            lowestReaderQueue.peek();
        KeyValue<ByteBuffer, ? extends Iterable<V>> kv = reader.peek();
        if (comparator.compare(kv.getKey(), key) != 0) {
          break;
        }
        if (kv.getValue().iterator().hasNext()) {
          return true;
        }
        consumePeekedValue(kv);
      }
      return false;
    }

    /**
     * @throws NoSuchElementException if there are no more values for this key.
     */
    @Override
    public V next() {
      if (currentValues.hasNext()) {
        return currentValues.next();
      }
      KeyValue<ByteBuffer, ? extends Iterable<V>> keyValue =
          (lowestReaderQueue.isEmpty()) ? null : lowestReaderQueue.peek().peek();
      if (keyValue == null || comparator.compare(keyValue.getKey(), key) != 0) {
        throw new NoSuchElementException();
      }
      consumePeekedValue(keyValue);
      currentValues = keyValue.getValue().iterator();
      return next();
    }

    /**
     * Helper to consume the value that was just peeked.
     */
    private void consumePeekedValue(KeyValue<ByteBuffer, ? extends Iterable<V>> peekedKeyValue) {
      PeekingInputReader<KeyValue<ByteBuffer, ? extends Iterable<V>>> reader =
          lowestReaderQueue.poll();
      consumePeekedValueFromReader(peekedKeyValue, reader);
      addReaderToQueueIfNotEmpty(reader);
    }
  }

  /**
   * @param peekedKeyValue The peeked value to be consumed
   * @param reader Where the value came from
   */
  private static <V> void consumePeekedValueFromReader(
      KeyValue<ByteBuffer, ? extends Iterable<V>> peekedKeyValue,
      PeekingInputReader<KeyValue<ByteBuffer, ? extends Iterable<V>>> reader) {
    if (reader == null || !peekedKeyValue.equals(reader.next())) {
      throw new ConcurrentModificationException("Reading from values is not threadsafe.");
    }
  }

  /**
   * Returns the next KeyValues object for the reducer. This is the entry point.
   * @throws NoSuchElementException if there are no more keys in the input.
   */
  @Override
  public KeyValue<K, Iterator<V>> next() throws NoSuchElementException {
    skipLeftoverItems();
    PeekingInputReader<KeyValue<ByteBuffer, ? extends Iterable<V>>> reader =
        lowestReaderQueue.remove();
    KeyValue<ByteBuffer, ? extends Iterable<V>> lowest = reader.next();
    ByteBuffer lowestKey = lowest.getKey();
    CombiningReader values = new CombiningReader(lowestKey, lowest.getValue());
    addReaderToQueueIfNotEmpty(reader);
    lastKey = SerializableValue.of(getByteBufferMarshaller(), lowestKey);
    return new KeyValue<K, Iterator<V>>(keyMarshaller.fromBytes(lowestKey.slice()), values);
  }

  private void addReaderToQueueIfNotEmpty(
      PeekingInputReader<KeyValue<ByteBuffer, ? extends Iterable<V>>> reader) {
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
      PeekingInputReader<KeyValue<ByteBuffer, ? extends Iterable<V>>> reader =
          lowestReaderQueue.remove();
      itemSkiped = skipItemsOnReader(reader);
      addReaderToQueueIfNotEmpty(reader);
    } while (itemSkiped);
  }

  /**
   * Helper function for skipLeftoverItems to skip items matching last key on a single reader.
   */
  private boolean skipItemsOnReader(
      PeekingInputReader<KeyValue<ByteBuffer, ? extends Iterable<V>>> reader) {
    boolean itemSkipped = false;
    KeyValue<ByteBuffer, ? extends Iterable<V>> keyValue = reader.peek();
    while (keyValue != null && comparator.compare(keyValue.getKey(), lastKey.getValue()) == 0) {
      consumePeekedValueFromReader(keyValue, reader);
      itemSkipped = true;
      keyValue = reader.peek();
    }
    return itemSkipped;
  }

  @Override
  public Double getProgress() {
    double total = 0;
    for (PeekingInputReader<?> reader : readers) {
      Double progress = reader.getProgress();
      if (progress != null) {
        total += progress;
      }
    }
    return total / readers.size();
  }

  @Override
  public void endShard() throws IOException {
    for (PeekingInputReader<?> reader : readers) {
      reader.endShard();
    }
  }

  @Override
  public long estimateMemoryRequirement() {
    long total = 0;
    for (PeekingInputReader<?> reader : readers) {
      total += reader.estimateMemoryRequirement();
    }
    return total;
  }
}
