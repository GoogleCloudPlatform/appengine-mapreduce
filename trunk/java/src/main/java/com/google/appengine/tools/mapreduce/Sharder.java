package com.google.appengine.tools.mapreduce;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Used to determine which shard an item belongs to.
 * This is used when emitting data from Map to specify which reduce shard it should go to.
 * The only criteria that is required is that the same key always map to the same shard.
 *
 */
public interface Sharder extends Serializable {

  /**
   * @return the number of shards that are items may be assigned to.
   */
  public int getNumShards();

  /**
   * @param key The serialized key. (The ByteBuffer should be unmodified by the implementation)
   * @return a number between 0 and numShards-1 inclusive
   */
  public int getShardForKey(ByteBuffer key);

}
