package com.google.appengine.demos.mapreduce.randomcollisions;

import com.google.appengine.tools.mapreduce.Reducer;
import com.google.appengine.tools.mapreduce.ReducerInput;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.logging.Logger;

/**
 * Counts the number of seeds that generated the same value. If there are multiple they will be
 * logged and emitted to the output as a list.
 */
public final class CollisionFindingReducer extends Reducer<Integer, Integer, ArrayList<Integer>> {

  private static final long serialVersionUID = 188147370819557065L;
  private static final Logger LOG = Logger.getLogger(CollisionFindingReducer.class.getName());
  @Override
  // [START reduce_example]
  public void reduce(Integer valueGenerated, ReducerInput<Integer> seeds) {
    ArrayList<Integer> collidingSeeds = Lists.newArrayList(seeds);
    if (collidingSeeds.size() > 1) {
      LOG.info("Found a collision! The seeds: " + collidingSeeds
          + " all generaged the value " + valueGenerated);
      emit(collidingSeeds);
    }
  }
  // [END reduce_example]
}
