package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.common.base.Charsets;
import com.google.common.primitives.Ints;

import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Tests for {@link HashingSharder}
 *
 */
public class HashingSharderTest extends TestCase {

  private interface KeyMaker {
    ByteBuffer createKey(Random r);
  }


  public void verifyWithKeyMaker(int numShards, KeyMaker k) {
    HashingSharder sharder = new HashingSharder(numShards);
    assertEquals(numShards, sharder.getNumShards());
    int targetPerShard = 10000;
    int[] counts = new int[numShards];
    Random r = new Random(0);
    for (int i = 0; i < counts.length * targetPerShard; i++) {
      int shard = sharder.getShardForKey(k.createKey(r));
      counts[shard]++;
    }
    int max = Ints.max(counts);
    int min = Ints.min(counts);
    assertTrue("Min: " + min + " max: " + max, min > targetPerShard * .9);
    assertTrue("Min: " + min + " max: " + max, max < targetPerShard * 1.1);
  }

  public void testRandomUniform() {
    KeyMaker keyMaker = new KeyMaker() {
      @Override
      public ByteBuffer createKey(Random r) {
        byte[] bytes = new byte[10];
        r.nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
      }
    };
    verifyWithKeyMaker(128, keyMaker);
    verifyWithKeyMaker(512, keyMaker);
  }

  public void testStringSequencesUniform() {
    KeyMaker keyMaker = new KeyMaker() {
      private int i = 0;

      @Override
      public ByteBuffer createKey(Random r) {
        return ByteBuffer.wrap(("Foo-" + i++).getBytes(Charsets.US_ASCII));
      }
    };
    verifyWithKeyMaker(100, keyMaker);
    verifyWithKeyMaker(10, keyMaker);
    verifyWithKeyMaker(2, keyMaker);
  }

  public void testIntegersUniform() {
    final Marshaller<Integer> marshaller = Marshallers.getIntegerMarshaller();
    KeyMaker keyMaker = new KeyMaker() {
      private int i = 0;

      @Override
      public ByteBuffer createKey(Random r) {
        return marshaller.toBytes(i++);
      }
    };
    verifyWithKeyMaker(100, keyMaker);
    verifyWithKeyMaker(10, keyMaker);
    verifyWithKeyMaker(2, keyMaker);
  }

}
