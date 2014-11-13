package com.google.appengine.tools.mapreduce.inputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.Marshaller;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

/**
 * An {@link InputReader} that unmarshals records.
 *
 * @param <T> type of values returned by this reader
 */
public class UnmarshallingInputReader<T> extends InputReader<T> {

  private static final long serialVersionUID = -5155146191805613155L;
  private final InputReader<ByteBuffer> reader;
  private final Marshaller<T> marshaller;

  public UnmarshallingInputReader(InputReader<ByteBuffer> reader, Marshaller<T> marshaller) {
    this.reader = checkNotNull(reader);
    this.marshaller = checkNotNull(marshaller);
  }

  public Marshaller<T> getMarshaller() {
    return marshaller;
  }

  @Override
  public T next() throws IOException, NoSuchElementException {
    ByteBuffer byteBuffer = reader.next();
    return marshaller.fromBytes(byteBuffer);
  }

  @Override
  public Double getProgress() {
    return reader.getProgress();
  }

  @Override
  public void beginShard() throws IOException {
    reader.beginShard();
  }

  @Override
  public void beginSlice() throws IOException {
    reader.beginSlice();
  }

  @Override
  public void endSlice() throws IOException {
    reader.endSlice();
  }

  @Override
  public void endShard() throws IOException {
    reader.endShard();
  }

  @Override
  public long estimateMemoryRequirement() {
    return reader.estimateMemoryRequirement();
  }
}
