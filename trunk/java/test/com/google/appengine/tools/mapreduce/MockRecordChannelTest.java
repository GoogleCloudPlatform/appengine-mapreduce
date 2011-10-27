// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

/**
 * Tests the {@link MockRecordChannel} to ensure that it properly
 * models a RecordReadChannel and RecordWriteChannel.
 *
 */
public final class MockRecordChannelTest extends TestCase {

  private MockRecordChannel mockChannel;
  private static final ByteBuffer SMALL_BUFFER = ByteBuffer.wrap(new byte[] { 41, 42, 43, 44, 45});

  @Override
  public void setUp() {
    mockChannel = new MockRecordChannel();
  }

  /**
   * Tests that a simple read and write works.
   * @throws Exception
   */
  public void testReadWrite() throws Exception {
    mockChannel.write(SMALL_BUFFER);
    assertEquals(SMALL_BUFFER, mockChannel.readRecord());
    assertNull(mockChannel.readRecord());
  }


  /**
   * Tests that multiple writes followed by reads works.
   * @throws Exception
   */
  public void testMultiple() throws Exception {
    ByteBuffer secondBuffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
    ByteBuffer thirdBuffer = ByteBuffer.wrap(new byte[] {4, 5, 6});

    mockChannel.write(SMALL_BUFFER);
    mockChannel.write(secondBuffer);
    mockChannel.write(thirdBuffer);

    assertEquals(SMALL_BUFFER, mockChannel.readRecord());
    assertEquals(secondBuffer, mockChannel.readRecord());
    assertEquals(thirdBuffer, mockChannel.readRecord());
    assertNull(mockChannel.readRecord());
  }

  /**
   * Tests that an open {@link MockRecordChannel} declares itself open.
   * @throws Exception
   */
  public void testIsOpen() throws Exception {
    assertTrue(mockChannel.isOpen());
  }

  /**
   * Tests that closing a {@link MockRecordChannel} marks it as closed.
   * @throws Exception
   */
  public void testClose() throws Exception {
    mockChannel.close();
    assertFalse(mockChannel.isOpen());
  }

  /**
   * Tests that closeFinally marks the {@link MockRecordChannel} as closed.
   * @throws Exception
   */
  public void testCloseFinally() throws Exception {
    mockChannel.closeFinally();
    assertFalse(mockChannel.isOpen());
  }

  /**
   * Tests that reading from a position works.
   * @throws Exception
   */
  public void testReadFromPreviousPosition() throws Exception {
    mockChannel.write(SMALL_BUFFER);
    ByteBuffer secondBuffer = ByteBuffer.wrap(new byte[] { 1, 2, 3, 4 });
    mockChannel.write(secondBuffer);
    long firstPosition = mockChannel.position();
    assertEquals(SMALL_BUFFER, mockChannel.readRecord());
    mockChannel.position(firstPosition);
    assertEquals(SMALL_BUFFER, mockChannel.readRecord());
    assertEquals(secondBuffer, mockChannel.readRecord());
  }

  /**
   * Tests that reading from a position works.
   * @throws Exception
   */
  public void testReadFromFuturePosition() throws Exception {
    mockChannel.write(SMALL_BUFFER);
    ByteBuffer secondBuffer = ByteBuffer.wrap(new byte[] { 1, 2, 3, 4 });
    mockChannel.write(secondBuffer);
    mockChannel.position(1); //the mock channel uses index based positions.
    assertEquals(secondBuffer, mockChannel.readRecord());
    assertNull(mockChannel.readRecord());
  }

  /**
   * Tests that writing after a {@link MockRecordChannel} is closed throws a
   * {@link ClosedChannelException}.
   * @throws Exception
   */
  public void testWriteToClosedChannel() throws Exception {
    mockChannel.close();
    try {
      mockChannel.write(SMALL_BUFFER);
      fail();
    } catch (ClosedChannelException expected) {
    }
  }

  /**
   * Tests that reading from a closed {@link MockRecordChannel} throws a
   * {@link ClosedChannelException}.
   * @throws Exception
   */
  public void testReadFromClosedChannel() throws Exception {
    mockChannel.write(SMALL_BUFFER);
    mockChannel.close();
    try {
      mockChannel.readRecord();
      fail();
    } catch (ClosedChannelException expected) {
    }
  }

  /**
   * Tests that getting the position after a {@link MockRecordChannel} is closed
   * throws a {@link ClosedChannelException}.
   * @throws Exception
   */
  public void testGetPositionFromClosedChannel() throws Exception {
    mockChannel.write(SMALL_BUFFER);
    mockChannel.close();
    try {
      mockChannel.position();
      fail();
    } catch (ClosedChannelException expected) {
    }
  }

  /**
   * Tests that setting the position after a {@link MockRecordChannel} is closed
   * throws a {@link ClosedChannelException}.
   * @throws Exception
   */
  public void testSetPositionOnClosedChannel() throws Exception {
    mockChannel.write(SMALL_BUFFER);
    mockChannel.close();
    try {
      mockChannel.position(0);
      fail();
    } catch (ClosedChannelException expected) {
    }
  }
}
