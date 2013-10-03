package com.google.appengine.tools.mapreduce.impl.sort;

import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.common.base.Charsets;

import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Random;

/**
 * Tests for {@link LexicographicalComparator}
 */
public class LexicographicalComparatorTest extends TestCase {

  public void testSelfCompare() {
    LexicographicalComparator comp = new LexicographicalComparator();
    ByteBuffer value = ByteBuffer.wrap("A long Test String".getBytes(Charsets.US_ASCII));
    assertEquals(0, comp.compare(value, value));
    value = Marshallers.getLongMarshaller().toBytes(-1L);
    assertEquals(0, comp.compare(value, value));
    value = Marshallers.getLongMarshaller().toBytes(0L);
    assertEquals(0, comp.compare(value, value));
    value = Marshallers.getLongMarshaller().toBytes(1L);
    assertEquals(0, comp.compare(value, value));

    value = ByteBuffer.wrap("A long Test String".getBytes(Charsets.US_ASCII));
    ByteBuffer same = ByteBuffer.wrap("A long Test String".getBytes(Charsets.US_ASCII));
    assertEquals(0, comp.compare(value, same));
    value = Marshallers.getLongMarshaller().toBytes(1234567890L);
    same = Marshallers.getLongMarshaller().toBytes(1234567890L);
    assertEquals(0, comp.compare(value, same));
  }

  public void testCompare() {
    LexicographicalComparator comp = new LexicographicalComparator();
    ByteBuffer left = ByteBuffer.wrap("502539523".getBytes(Charsets.US_ASCII));
    ByteBuffer right = ByteBuffer.wrap("-1930858313".getBytes(Charsets.US_ASCII));
    assertTrue(comp.compare(left, right) > 0);
    left = ByteBuffer.wrap("foo".getBytes(Charsets.US_ASCII));
    right = ByteBuffer.wrap("bar".getBytes(Charsets.US_ASCII));
    assertTrue(comp.compare(left, right) > 0);
    left = ByteBuffer.wrap("A".getBytes(Charsets.US_ASCII));
    right = ByteBuffer.wrap("Z".getBytes(Charsets.US_ASCII));
    assertTrue(comp.compare(left, right) < 0);
    left = ByteBuffer.wrap("".getBytes(Charsets.US_ASCII));
    right = ByteBuffer.wrap("x".getBytes(Charsets.US_ASCII));
    assertTrue(comp.compare(left, right) < 0);
    left = ByteBuffer.wrap("x".getBytes(Charsets.US_ASCII));
    right = ByteBuffer.wrap("".getBytes(Charsets.US_ASCII));
    assertTrue(comp.compare(left, right) > 0);
    left = ByteBuffer.wrap("0".getBytes(Charsets.US_ASCII));
    right = ByteBuffer.wrap("0".getBytes(Charsets.US_ASCII));
    assertTrue(comp.compare(left, right) == 0);
    left = ByteBuffer.wrap("00".getBytes(Charsets.US_ASCII));
    right = ByteBuffer.wrap("0".getBytes(Charsets.US_ASCII));
    assertTrue(comp.compare(left, right) > 0);
  }

  public void testEpilogue() {
    LexicographicalComparator comp = new LexicographicalComparator();
    Random r = new Random(0);
    for (int i = 0; i < 100000; i++) {
      ByteBuffer b1 = ByteBuffer.allocate(12);
      ByteBuffer b2 = ByteBuffer.allocate(12);
      ByteBuffer prefix = Marshallers.getLongMarshaller().toBytes(r.nextLong());
      b1.put(prefix);
      prefix.rewind();
      b2.put(prefix);
      assertEquals(4, b1.remaining());
      assertEquals(4, b2.remaining());
      int i1 = r.nextInt();
      int i2 = r.nextInt();
      ByteBuffer suffix1 = Marshallers.getIntegerMarshaller().toBytes(i1);
      ByteBuffer suffix2 = Marshallers.getIntegerMarshaller().toBytes(i2);
      b1.put(suffix1);
      b2.put(suffix2);
      assertEquals(0, b1.remaining());
      assertEquals(0, b2.remaining());
      b1.rewind();
      b2.rewind();
      assertEquals(i1 > i2, comp.compare(b1, b2) > 0);
      assertEquals(i1 < i2, comp.compare(b1, b2) < 0);
    }
  }

  public void testRandomStrings() {
    LexicographicalComparator comp = new LexicographicalComparator();
    Random r = new Random(0);
    for (int i = 0; i < 100000; i++) {
      String s1 = getRandomString(r);
      String s2 = getRandomString(r);
      ByteBuffer b1 = Charsets.UTF_8.encode(s1);
      ByteBuffer b2 = Charsets.UTF_8.encode(s2);
      assertEquals(s1.compareTo(s2) > 0, comp.compare(b1, b2) > 0);
      assertEquals(s1.compareTo(s2) < 0, comp.compare(b1, b2) < 0);
    }
  }

  private String getRandomString(Random r) {
    CharBuffer chars = CharBuffer.allocate(10);
    while (chars.hasRemaining()) {
      chars.append((char) r.nextInt());
    } // Round-triping the data so as to transform out any invalid sequences
    return Charsets.UTF_8.decode(Charsets.UTF_8.encode(chars)).toString();
  }

  public void testNumbersCompare() {
    LexicographicalComparator comp = new LexicographicalComparator();
    ByteBuffer min = Marshallers.getIntegerMarshaller().toBytes(Integer.MIN_VALUE);
    ByteBuffer negTwo = Marshallers.getIntegerMarshaller().toBytes(-2);
    ByteBuffer negOne = Marshallers.getIntegerMarshaller().toBytes(-1);
    ByteBuffer zero = Marshallers.getIntegerMarshaller().toBytes(0);
    ByteBuffer one = Marshallers.getIntegerMarshaller().toBytes(1);
    ByteBuffer two = Marshallers.getIntegerMarshaller().toBytes(2);
    ByteBuffer twoFiftyFive = Marshallers.getIntegerMarshaller().toBytes(255);
    ByteBuffer twoFiftySix = Marshallers.getIntegerMarshaller().toBytes(256);
    ByteBuffer max = Marshallers.getIntegerMarshaller().toBytes(Integer.MAX_VALUE);
    assertTrue(comp.compare(min, negTwo) < 0);
    assertTrue(comp.compare(negTwo, negOne) < 0);
    assertTrue(comp.compare(negOne, zero) < 0);
    assertTrue(comp.compare(zero, one) < 0);
    assertTrue(comp.compare(one, two) < 0);
    assertTrue(comp.compare(two, twoFiftyFive) < 0);
    assertTrue(comp.compare(twoFiftyFive, twoFiftySix) < 0);
    assertTrue(comp.compare(twoFiftySix, max) < 0);
    assertTrue(comp.compare(min, max) < 0);

    min = Marshallers.getLongMarshaller().toBytes(Long.MIN_VALUE);
    negTwo = Marshallers.getLongMarshaller().toBytes(-2L);
    negOne = Marshallers.getLongMarshaller().toBytes(-1L);
    zero = Marshallers.getLongMarshaller().toBytes(0L);
    one = Marshallers.getLongMarshaller().toBytes(1L);
    two = Marshallers.getLongMarshaller().toBytes(2L);
    twoFiftyFive = Marshallers.getLongMarshaller().toBytes(255L);
    twoFiftySix = Marshallers.getLongMarshaller().toBytes(256L);
    max = Marshallers.getLongMarshaller().toBytes(Long.MAX_VALUE);
    assertTrue(comp.compare(min, negTwo) < 0);
    assertTrue(comp.compare(negTwo, negOne) < 0);
    assertTrue(comp.compare(negOne, zero) < 0);
    assertTrue(comp.compare(zero, one) < 0);
    assertTrue(comp.compare(one, two) < 0);
    assertTrue(comp.compare(two, twoFiftyFive) < 0);
    assertTrue(comp.compare(twoFiftyFive, twoFiftySix) < 0);
    assertTrue(comp.compare(twoFiftySix, max) < 0);
    assertTrue(comp.compare(min, max) < 0);
  }

}
