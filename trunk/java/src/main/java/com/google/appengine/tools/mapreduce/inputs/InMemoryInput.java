package com.google.appengine.tools.mapreduce.inputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * An input that returns values already in memory.
 *
 * @param <I> type of values returned by this input
 */
public class InMemoryInput<I> extends Input<I> {

  private static final long serialVersionUID = -7058791377469359722L;
  private final List<InMemoryInputReader<I>> readers;

  private static final class InMemoryInputReader<I> extends InputReader<I> {
    private static final long serialVersionUID = -7442905939930896134L;
    int pos = 0;
    private List<I> results;

    InMemoryInputReader(List<I> results) {
      this.results = ImmutableList.copyOf(results);
    }

    @Override
    public I next() throws IOException, NoSuchElementException {
      if (pos >= results.size()) {
        throw new NoSuchElementException();
      }
      return results.get(pos++);
    }

    @Override
    public Double getProgress() {
      if (results.isEmpty()) {
        return 1.0;
      }
      return ((double) pos) / results.size();
    }

  }

  public InMemoryInput(List<List<I>> input) {
    checkNotNull(input, "Null input");
    Builder<InMemoryInputReader<I>> builder = ImmutableList.builder();
    for (List<I> shard : input) {
      builder.add(new InMemoryInputReader<I>(shard));
    }
    readers = builder.build();
  }

  @Override
  public List<? extends InputReader<I>> createReaders() throws IOException {
    return readers;
  }
}
