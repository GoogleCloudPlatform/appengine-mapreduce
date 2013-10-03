package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.appengine.tools.mapreduce.Sharder;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.ByteBuffer;

/**
 * Splits input by hashing the key.
 *
 */
public class HashingSharder implements Sharder {

  private static final long serialVersionUID = 7967187256546710108L;
  private static final HashFunction HASH = Hashing.murmur3_32();
  private int numShards;

  public HashingSharder(int numShards) {
    this.numShards = numShards;
    checkArgument(numShards > 0);
  }

  @Override
  public int getNumShards() {
    return numShards;
  }

  @Override
  public int getShardForKey(ByteBuffer key) {
    byte[] bytes = SerializationUtil.getBytes(key);
    int hash = (HASH.hashBytes(bytes).asInt()) & Integer.MAX_VALUE; // Keeping positive
    // Dividing integer range rather than using modulo so as to avoid rewriting entries if they are
    // re-hashed.
    return hash / (Integer.MAX_VALUE / numShards + 1);
  }

}
