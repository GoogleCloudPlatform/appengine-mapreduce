package com.google.appengine.tools.mapreduce.impl.sort;

import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Worker;
import com.google.appengine.tools.mapreduce.impl.handlers.MemoryLimiter;
import com.google.appengine.tools.mapreduce.impl.shardedjob.RejectRequestException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Sorts a set of keyValues by a lexicographical comparison of the bytes of the key. On beginSlice a
 * large buffer is allocated to store the data.
 * <p>
 * The data is stored in the values buffer. Once this is full the data is sorted in place and
 * written out. The class cannot be used again until beginSlice is called again.
 * <p>
 * The in place sort is achieved as follows: Each new item is added to the beginning of the buffer,
 * and a pointer is written to the end. Once the buffer is filled the pointers at the end are sorted
 * where the order is determined by comparing the keys they point to. Once they are ordered the
 * data can be read out by reading through the pointers and emitting the value that corresponds to
 * them.
 * <p>
 * This class is NOT threadSafe.
 *
 */
public class SortWorker extends Worker<SortContext> {

  private static final long serialVersionUID = 5872735741738296902L;
  private static final Logger log = Logger.getLogger(SortWorker.class.getName());

  private static final long SORT_MEMORY_OVERHEAD = 8 * 1024 * 1024; // Estimate.

  /**
   * Fraction of system ram sort will allocate. There are multiple values in case the largest
   * proportion is unavailable. If the smallest is unavailable sort will fail.
   */
  private static final double[] TARGET_SORT_RAM_PROPORTIONS = {0.25, 0.15};
  private static final int MEMORY_ALLOCATION_ATTEMPTS = TARGET_SORT_RAM_PROPORTIONS.length;

  // Items are batched to save storage cost, but not too big to limit memory use.
  static final int BATCHED_ITEM_SIZE_PER_EMIT = 1024;

  static final int POINTER_SIZE_BYTES = 3 * 4; // 3 ints: KeyIndex, ValueIndex, Length

  private transient ByteBuffer memoryBuffer;
  private transient int valuesHeld;
  private transient KeyValue<ByteBuffer, ByteBuffer> leftover;
  private transient boolean isFull;
  private transient LexicographicalComparator comparator;

  private final class IndexedComparator implements IntComparator {

    @Override
    public int compare(int a, int b) {
      int origionalLimit = memoryBuffer.limit();
      memoryBuffer.limit(memoryBuffer.capacity());
      int pointerOffset = computePointerOffset(a);
      int aPos = memoryBuffer.getInt(pointerOffset);
      int aLen = memoryBuffer.getInt(pointerOffset + 4) - aPos;
      pointerOffset = computePointerOffset(b);
      int bPos = memoryBuffer.getInt(pointerOffset);
      int bLen = memoryBuffer.getInt(pointerOffset + 4) - bPos;
      memoryBuffer.limit(origionalLimit);
      return LexicographicalComparator.compare(memoryBuffer, aPos, aLen, memoryBuffer, bPos, bLen);
    }

    @Override
    public int compare(Integer a, Integer b) {
      return compare(a.intValue(), b.intValue());
    }
  }

  private final class IndexedSwapper implements Swapper {
    @Override
    public void swap(int a, int b) {
      swapPointers(a, b);
    }
  }

  void prepare() {
    memoryBuffer = allocateMemory();
  }

  void release() {
    memoryBuffer = null;
  }

  @Override
  public void beginSlice() {
    comparator = new LexicographicalComparator();
    assert memoryBuffer != null;
    valuesHeld = 0;
    leftover = null;
    isFull = false;
  }

  @Override
  public void endSlice() {
    Stopwatch stopwatch = Stopwatch.createStarted();
    sortData();
    log.info(
        "Sorted " + valuesHeld + " items in " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
    try {
      stopwatch.reset().start();
      writeOutData();
      log.info("Wrote " + getStoredSize() + " bytes of data in "
          + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private int getStoredSize() {
    return memoryBuffer.position();
  }

  /**
   * Re arranges the pointers so that they are ordered according to the order of the corresponding
   * keys.
   */
  private void sortData() {
    Arrays.quickSort(0, valuesHeld, new IndexedComparator(), new IndexedSwapper());
  }

  /**
   * Writes out the key value pairs in order.
   * If there are multiple consecutive values with the same key, they can be combined to avoid
   * repeating the key.
   * In the event the buffer is full, there is one leftover item which did not go into it,
   * and hence was not sorted. So a merge between this one item and the sorted list is done on the
   * way out.
   */
  private void writeOutData() throws IOException {
    if (valuesHeld == 0) {
      return;
    }
    SortContext localContext = getContext();

    ByteBuffer currentKey = getKeyValueFromPointer(0).getKey();
    List<ByteBuffer> currentValues = new ArrayList<>();
    int totalSize = 0;

    for (int i = 0; i < valuesHeld; i++) {
      KeyValue<ByteBuffer, ByteBuffer> keyValue = getKeyValueFromPointer(i);
      int compare = comparator.compare(keyValue.getKey(), currentKey);

      if (compare == 0) {
        if (totalSize > BATCHED_ITEM_SIZE_PER_EMIT) {
          emitCurrentOrLeftover(localContext, currentKey, currentValues);
          totalSize = 0;
        }
        currentValues.add(keyValue.getValue());
        totalSize += keyValue.getValue().remaining();
      } else if (compare > 0) {
        emitCurrentOrLeftover(localContext, currentKey, currentValues);
        currentKey = keyValue.getKey();
        currentValues.add(keyValue.getValue());
        totalSize = keyValue.getValue().remaining();
      } else {
        throw new IllegalStateException("Sort failed to properly order output");
      }
    }
    emitCurrentOrLeftover(localContext, currentKey, currentValues);
    if (leftover != null) {
      localContext.emit(leftover.getKey(), ImmutableList.of(leftover.getValue()));
    }
  }

  /**
   * Writes out the provided key and value list. If the leftover item (which was not included in the
   * sort) is lower lexicographically then it is emitted first.
   *
   * If the values are emitted the list will be cleared. If the leftover value is emitted the
   * leftover value is cleared.
   *
   * @param key The key being asked to be emitted. (Will not be modified)
   * @param values the values associated with the key that should be emitted.
   */
  private void emitCurrentOrLeftover(
      SortContext localContext, ByteBuffer key, List<ByteBuffer> values) throws IOException {
    if (leftover != null) {
      int leftOverCompare = comparator.compare(leftover.getKey(), key);
      if (leftOverCompare <= 0) {
        localContext.emit(leftover.getKey(), ImmutableList.of(leftover.getValue()));
        leftover = null;
      }
    }
    if (!values.isEmpty()) {
      localContext.emit(key, new ArrayList<>(values));
      values.clear();
    }
  }

  /**
   * @return false iff no more items may be added.
   */
  public boolean isFull() {
    return isFull;
  }

  /**
   * Add a new key and value to the in memory buffer.
   */
  public void addValue(ByteBuffer key, ByteBuffer value) {
    if (isFull) {
      throw new IllegalArgumentException("Already full");
    }
    if (value.remaining() + key.remaining() + POINTER_SIZE_BYTES > memoryBuffer.remaining()) {
      leftover = new KeyValue<>(key, value);
      isFull = true;
    } else {
      int keyPos = spliceIn(key, memoryBuffer);
      int valuePos = spliceIn(value, memoryBuffer);
      addPointer(keyPos, key.remaining(), valuePos, value.remaining());
    }
  }

  /**
   * Get a key given the index of its pointer.
   */
  final ByteBuffer getKeyFromPointer(int index) {
    int origionalLimit = memoryBuffer.limit();
    memoryBuffer.limit(memoryBuffer.capacity());
    int pointerOffset = computePointerOffset(index);
    int keyPos =  memoryBuffer.getInt(pointerOffset);
    int valuePos =  memoryBuffer.getInt(pointerOffset + 4);
    memoryBuffer.limit(origionalLimit);
    assert valuePos >= keyPos;
    ByteBuffer key = sliceOutRange(keyPos, valuePos);
    return key;
  }

  /**
   * Get a key and its value given the index of its pointer.
   */
  final KeyValue<ByteBuffer, ByteBuffer> getKeyValueFromPointer(int index) {
    int origionalLimit = memoryBuffer.limit();
    memoryBuffer.limit(memoryBuffer.capacity());
    int pointerOffset = computePointerOffset(index);
    int keyPos =  memoryBuffer.getInt(pointerOffset);
    int valuePos =  memoryBuffer.getInt(pointerOffset + 4);
    int valueLength = memoryBuffer.getInt(pointerOffset + 8);
    memoryBuffer.limit(origionalLimit);
    assert valuePos >= keyPos;
    ByteBuffer key = sliceOutRange(keyPos, valuePos);
    ByteBuffer value = sliceOutRange(valuePos, valuePos + valueLength);
    return new KeyValue<>(key, value);
  }

  /**
   * @param beginPos absolute position to read from.
   * @param limitPos absolute position of the limit to be read to.
   * @return a ByteBuffer that points to the specified range in the values buffer.
   */
  private ByteBuffer sliceOutRange(int beginPos, int limitPos) {
    int origPos = memoryBuffer.position();
    int origLimit = memoryBuffer.limit();
    memoryBuffer.limit(limitPos);
    memoryBuffer.position(beginPos);
    ByteBuffer result = memoryBuffer.slice();
    memoryBuffer.limit(origLimit);
    memoryBuffer.position(origPos);
    return result;
  }

  /**
   * Place the pointer at indexA in indexB and vice versa.
   */
  final void swapPointers(int indexA, int indexB) {
    assert indexA >= 0 && indexA < valuesHeld;
    assert indexB >= 0 && indexB < valuesHeld;

    ByteBuffer a = copyPointer(indexA);
    ByteBuffer b = readPointer(indexB);
    writePointer(indexA, b);
    writePointer(indexB, a);
  }

  /**
   * Write the provided pointer at the specified index.
   * (Assumes limit on buffer is correctly set)
   * (Position of the buffer changed)
   */
  private void writePointer(int index, ByteBuffer pointer) {
    int limit = memoryBuffer.limit();
    int pos = memoryBuffer.position();
    int pointerOffset = computePointerOffset(index);
    memoryBuffer.limit(pointerOffset + POINTER_SIZE_BYTES);
    memoryBuffer.position(pointerOffset);
    memoryBuffer.put(pointer);
    memoryBuffer.limit(limit);
    memoryBuffer.position(pos);
  }

  /**
   * Given an index for a pointer returns it's offset in memoryBuffer
   */
  private final int computePointerOffset(int index) {
    return memoryBuffer.capacity() - (index + 1) * POINTER_SIZE_BYTES;
  }

  /**
   * Read a pointer from the specified index.
   */
  final ByteBuffer readPointer(int index) {
    int pointerOffset = computePointerOffset(index);
    return sliceOutRange(pointerOffset, pointerOffset + POINTER_SIZE_BYTES);
  }

  /**
   * Get a Copy of a pointer
   */
  final ByteBuffer copyPointer(int index) {
    ByteBuffer pointer = readPointer(index);
    // Making a copy for so that someone can modify the underlying impl
    ByteBuffer result = ByteBuffer.allocate(pointer.capacity());
    result.put(pointer);
    result.flip();
    return result;
  }

  /**
   * Add a pointer to the key value pair with the provided parameters.
   */
  final void addPointer(int keyPos, int keySize, int valuePos, int valueSize) {
    assert keyPos + keySize == valuePos;
    int start = memoryBuffer.limit() - POINTER_SIZE_BYTES;
    memoryBuffer.putInt(start, keyPos);
    memoryBuffer.putInt(start + 4, valuePos);
    memoryBuffer.putInt(start + 8, valueSize);
    memoryBuffer.limit(start);
    valuesHeld++;
  }

  /**
   * Write the contents of src to dest, without messing with src's position.
   *
   * @param dest (position is advanced)
   * @return the pos in dest where src is written
   */
  private static int spliceIn(ByteBuffer src, ByteBuffer dest) {
    int position = dest.position();
    int srcPos = src.position();
    dest.put(src);
    src.position(srcPos);
    return position;
  }

  /**
   * This attempts to allocate as much memory as can be claimed for sorting. Ideally this should be
   * as large as possible. However because there may be multiple requests occurring on the same
   * instance, several attempts may be made to allocate a large portion.
   *
   * @throws RuntimeException If we cannot allocate after several attempts.
   */
  @VisibleForTesting
  ByteBuffer allocateMemory() {
    Runtime runtime = Runtime.getRuntime();
    for (int retries = 0; retries < MEMORY_ALLOCATION_ATTEMPTS; retries++) {
      int targetCapacity = getMemoryForSort(retries);
      try {
        return ByteBuffer.allocateDirect(targetCapacity);
      } catch (OutOfMemoryError e) {
        log.info("Failed to allocate direct memory for sort: " + targetCapacity
            + " retrying with a smaller buffer.");
      }
    }
    int targetCapacity = getMemoryForSort(MEMORY_ALLOCATION_ATTEMPTS);
    try {
      if (targetCapacity < runtime.freeMemory() + (runtime.maxMemory() - runtime.totalMemory())) {
        log.info("Using indirect memory allocation.");
        return ByteBuffer.allocate(targetCapacity);
      } else {
        log.info("Skipping indirect memory allocation.");
      }
    } catch (OutOfMemoryError e) {
      log.info("Failed to allocate non-direct memory for sort: " + targetCapacity + " giving up");
    }
    throw new RejectRequestException("Failed to allocate memory for sort after "
        + MEMORY_ALLOCATION_ATTEMPTS + " attempts. Giving up.");
  }

  private static int getMemoryForSort(int numRetries) {
    long maxUsableMemory = MemoryLimiter.TOTAL_CLAIMABLE_MEMORY_SIZE_MB * 1024L * 1024L;
    int memIndex = Math.min(numRetries, MEMORY_ALLOCATION_ATTEMPTS - 1);
    return Ints.saturatedCast((long) (maxUsableMemory * TARGET_SORT_RAM_PROPORTIONS[memIndex]));
  }

  @Override
  public long estimateMemoryRequirement() {
    return getMemoryForSort(0) + SORT_MEMORY_OVERHEAD;
  }

  public int getValuesHeld() {
    return valuesHeld;
  }
}
