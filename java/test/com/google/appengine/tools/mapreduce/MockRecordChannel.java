// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.files.RecordReadChannel;
import com.google.appengine.api.files.RecordWriteChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A channel that mocks a RecordReadChannel and RecordWriteChannel that are
 * wrapped around the same file.
 *
 */
public class MockRecordChannel implements RecordReadChannel, RecordWriteChannel {

  private final Deque<ByteBuffer> previousData;
  private final Deque<ByteBuffer> data;
  private int position;
  private boolean finalized;
  private boolean closed;
  
  /**
   * Creates a new {@link MockRecordChannel}
   */
  public MockRecordChannel() {
    previousData = new ArrayDeque<ByteBuffer>();
    data = new ArrayDeque<ByteBuffer>();
    position = 0;
    finalized = false;
    closed = false;
  }
  
  
  /**
   * {@inheritDoc}
   */
  @Override
  public int write(ByteBuffer src) throws IOException {
    if (closed) {
      throw new ClosedChannelException();
    }
    data.add(src);
    int read = src.remaining();
    return read;
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isOpen() {
    return !closed;
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    closed = true;
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public int write(ByteBuffer src, String sequenceKey) throws IOException {
    return write(src);
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public void closeFinally() throws IllegalStateException, IOException {
    finalized = true;
    closed = true;
  }  
  
  /**
   * {@inheritDoc}
   */
  @Override
  public ByteBuffer readRecord() throws IOException {
    if (closed) {
      throw new ClosedChannelException();
    }
    if (data.isEmpty()) {
      return null;
    }
    position++;
    previousData.add(data.poll());
    return previousData.peekLast();
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public long position() throws IOException {
    if (closed) {
      throw new ClosedChannelException();
    }
    return position;
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public void position(long newPosition) throws IOException {
    if (closed) {
      throw new ClosedChannelException();
    }
    while (newPosition > position && !data.isEmpty()) {
      previousData.add(data.poll());
      position++;
    }
    while (newPosition < position && !previousData.isEmpty()) {
      data.addFirst(previousData.getLast());
      position--;
    }
  }

}