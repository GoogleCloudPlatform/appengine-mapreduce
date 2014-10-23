package com.google.appengine.tools.mapreduce.impl.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Convenience methods related to splitting.
 */
public class SplitUtil {

  public static <X> List<List<X>> split(List<X> input, int numSplits, boolean randomize) {
    ArrayList<X> toSplit = new ArrayList<>(input);
    if (randomize) {
      Collections.shuffle(toSplit, new Random(0L)); // Fixing seed for determinism
    }
    int minItemsPerShard = input.size() / numSplits;
    int remainder = input.size() % numSplits;
    ArrayList<List<X>> result = new ArrayList<>();
    int posInList = 0;
    for (int shard = 0; shard < numSplits; shard++) {
      int numItems = shard < remainder ? minItemsPerShard + 1 : minItemsPerShard;
      if (numItems > 0) {
        result.add(new ArrayList<>(toSplit.subList(posInList, posInList + numItems)));
        posInList += numItems;
      }
    }
    if (posInList != toSplit.size()) {
      throw new IllegalStateException(); // Impossible
    }
    return result;
  }
}
