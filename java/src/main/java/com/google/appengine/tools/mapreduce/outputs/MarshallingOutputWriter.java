package com.google.appengine.tools.mapreduce.outputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.OutputWriter;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An {@link OutputWriter} that marshalls records.
 *
 * @param <O> the type of OutputWriter that this will become. (The type of the values that will be
 *        written to this class)
 */
public class MarshallingOutputWriter<O> extends ForwardingOutputWriter<O> {

  private static final long serialVersionUID = -1441650908652534613L;
  private final Marshaller<O> marshaller;
  private final OutputWriter<ByteBuffer> writer;

  public MarshallingOutputWriter(OutputWriter<ByteBuffer> writer, Marshaller<O> marshaller) {
    this.writer = checkNotNull(writer, "No writer");
    this.marshaller = checkNotNull(marshaller, "No marshaller");

  }
  @Override
  protected OutputWriter<ByteBuffer> getDelegate() {
    return writer;
  }

  @Override
  public void write(O value) throws IOException {
    ByteBuffer bytes = marshaller.toBytes(value);
    writer.write(bytes);
  }

}
