// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.outputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.logging.Logger;

/**
 * An output that accepts objects, invokes {@link #toString()} (or an arbitrary
 * other String-valued {@link Function} that is {@link Serializable}) on them,
 * adds an optional terminator, encodes the resulting String according to a
 * {@link Charset}, and writes the result to an underlying {@link Output}.
 *
 * Unmappable characters will be replaced (see {@link java.nio.charset.CodingErrorAction#REPLACE}).
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <O> type of values accepted by this output
 * @param <R> type of result returned by the underlying output
 */
public class StringOutput<O, R> extends Output<O, R> {
  private static final long serialVersionUID = 390838532348847158L;

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(StringOutput.class.getName());

  private static class Writer<O> extends ForwardingOutputWriter<O> {
    private static final long serialVersionUID = 142086167097140914L;

    private final OutputWriter<ByteBuffer> out;
    private final Function<O, String> fn;
    private final String terminator;
    private final String charsetName;

    private transient Charset charset;

    Writer(OutputWriter<ByteBuffer> out,
        Function<O, String> fn,
        String terminator,
        String charsetName) {
      this.out = checkNotNull(out, "Null out");
      this.fn = checkNotNull(fn, "Null fn");
      this.terminator = checkNotNull(terminator, "Null terminator");
      this.charsetName = checkNotNull(charsetName, "Null charsetName");
    }

    @Override public void beginSlice() throws IOException {
      super.beginSlice();
      charset = Charset.forName(charsetName);
    }

    @Override protected OutputWriter<?> getDelegate() {
      return out;
    }

    @Override public void write(O value) throws IOException {
      out.write(charset.encode(fn.apply(value) + terminator));
    }
  }

  private static class ToStringFn<O> implements Function<O, String>, Serializable {
    private static final long serialVersionUID = 158579098752936256L;

    @Override public String apply(O in) {
      return "" + in;
    }
  }

  private final Function<O, String> fn;
  private final String terminator;
  // Using charset name rather than charset for serializability.
  private final String charsetName;
  private final Output<ByteBuffer, R> sink;

  public StringOutput(Function<O, String> fn,
      String terminator,
      String charsetName,
      Output<ByteBuffer, R> sink) {
    this.fn = checkNotNull(fn, "Null fn");
    this.terminator = checkNotNull(terminator, "Null terminator");
    this.charsetName = checkNotNull(charsetName, "Null charsetName");
    this.sink = checkNotNull(sink, "Null sink");
    // Validate charset name eagerly.
    Charset.forName(charsetName);
  }

  public StringOutput(String terminator, String charsetName, Output<ByteBuffer, R> sink) {
    this(new ToStringFn<O>(), terminator, charsetName, sink);
  }

  public StringOutput(String terminator, Output<ByteBuffer, R> sink) {
    this(terminator, "UTF-8", sink);
  }

  @Override public List<? extends OutputWriter<O>> createWriters() throws IOException {
    List<? extends OutputWriter<ByteBuffer>> sinkWriters = sink.createWriters();
    ImmutableList.Builder<Writer<O>> out = ImmutableList.builder();
    for (OutputWriter<ByteBuffer> sinkWriter : sinkWriters) {
      out.add(new Writer<O>(sinkWriter, fn, terminator, charsetName));
    }
    return out.build();
  }

  /**
   * Returns whatever the underlying {@code Output}'s {@link #finish} method returns.
   */
  @Override public R finish(List<? extends OutputWriter<O>> writers) throws IOException {
    ImmutableList.Builder<OutputWriter<ByteBuffer>> sinkWriters = ImmutableList.builder();
    for (OutputWriter<O> w : writers) {
      Writer<O> writer = (Writer<O>) w;
      sinkWriters.add(writer.out);
    }
    return sink.finish(sinkWriters.build());
  }

}
