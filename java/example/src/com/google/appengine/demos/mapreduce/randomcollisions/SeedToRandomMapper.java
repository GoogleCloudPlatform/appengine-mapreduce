package com.google.appengine.demos.mapreduce.randomcollisions;

import com.google.appengine.tools.mapreduce.Mapper;
import com.google.common.primitives.Ints;

import java.util.Random;

/**
 * Maps each incoming seed using Java's Random to the first generated number. 
 */
final class SeedToRandomMapper extends Mapper<Long, Integer, Integer> {

  private static final long serialVersionUID = -3070710020513042698L;

  @Override
  public void map(Long sequence) {
    Random r = new Random(sequence);
    getContext().emit(r.nextInt(), Ints.checkedCast(sequence));
  }
}