package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.common.base.Charsets;
import com.google.common.primitives.Ints;

import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
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

  public void testSubdevision() {
    testSubdevision(2, 8);
    testSubdevision(3, 9);
    testSubdevision(8, 256);
    testSubdevision(9, 81);
    testSubdevision(10, 20);
    testSubdevision(10, 100);
    testSubdevision(64, 256);
    testSubdevision(90, 256);
    testSubdevision(101, 10000);
    testSubdevision(128, 1024);
    testSubdevision(128, 10000);
    testSubdevision(1000, 10000);
  }

  private void testSubdevision(int numInitialShards, int numRehashedShards) {
    int numItems = Math.min(10000, numInitialShards * numRehashedShards * 2);
    final Marshaller<Integer> marshaller = Marshallers.getIntegerMarshaller();
    HashingSharder sharder = new HashingSharder(numInitialShards);
    ArrayList<ArrayList<Integer>> selectedShards =
        new ArrayList<ArrayList<Integer>>(numInitialShards);
    for (int i = 0; i < numInitialShards; i++) {
      selectedShards.add(new ArrayList<Integer>());
    }
    for (int i = 0; i < numItems; i++) {
      ArrayList<Integer> intsOnShard =
          selectedShards.get(sharder.getShardForKey(marshaller.toBytes(i)));
      intsOnShard.add(i);
    }

    sharder = new HashingSharder(numRehashedShards);
    for (ArrayList<Integer> initialShard : selectedShards) {
      Map<Integer, Integer> rehashedShardCount = new HashMap<Integer, Integer>(numRehashedShards);
      for (Integer item : initialShard) {
        int newShard = sharder.getShardForKey(marshaller.toBytes(item));
        Integer intsOnShard = rehashedShardCount.get(newShard);
        if (intsOnShard == null) {
          rehashedShardCount.put(newShard, 1);
        } else {
          rehashedShardCount.put(newShard, intsOnShard + 1);
        }
      }
      double expectNum = ((double) numRehashedShards) / numInitialShards;
      assertTrue("Expected about, " + expectNum + " but found " + rehashedShardCount.size(),
          rehashedShardCount.size() <= Math.ceil(expectNum) + 1);
    }
  }

}
