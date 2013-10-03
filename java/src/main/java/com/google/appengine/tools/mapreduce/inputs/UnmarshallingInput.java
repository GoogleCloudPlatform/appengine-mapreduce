package com.google.appengine.tools.mapreduce.inputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * An {@link Input} that unmarshalls records.
 *
 * @param <I> type of values produced by this input
 */
public class UnmarshallingInput<I> extends Input<I> {
  private static final long serialVersionUID = 6893854789021758519L;
  private final Input<ByteBuffer> input;
  private final Marshaller<I> marshaller;

  /**
   * @param input The input producing values to unmarshall.
   * @param marshaller The marshaller to use for unmarshalling the input values.
   */
  public UnmarshallingInput(Input<ByteBuffer> input, Marshaller<I> marshaller) {
    this.input = checkNotNull(input, "Null input");
    this.marshaller = checkNotNull(marshaller, "Null marshaller");
  }

  @Override
  public List<InputReader<I>> createReaders() throws IOException {
    List<? extends InputReader<ByteBuffer>> readers = input.createReaders();
    List<InputReader<I>> result = Lists.newArrayListWithCapacity(readers.size());
    for (InputReader<ByteBuffer> reader : readers) {
      result.add(new UnmarshallingInputReader<I>(reader, marshaller));
    }
    return result;
  }
}
