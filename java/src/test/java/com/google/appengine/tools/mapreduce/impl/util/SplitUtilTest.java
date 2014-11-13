package com.google.appengine.tools.mapreduce.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.common.collect.ImmutableList;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RunWith(BlockJUnit4ClassRunner.class)
public class SplitUtilTest {

  @Test
  public void testSplitEven() {
    List<List<Integer>> list = SplitUtil.split(ImmutableList.of(1, 2, 3, 4, 5, 6), 3, false);
    assertEquals(3, list.size());
    assertEquals(2, list.get(0).size());
    assertEquals(2, list.get(1).size());
    assertEquals(2, list.get(2).size());

    assertEquals(1, list.get(0).get(0).intValue());
    assertEquals(2, list.get(0).get(1).intValue());
    assertEquals(3, list.get(1).get(0).intValue());
    assertEquals(4, list.get(1).get(1).intValue());
    assertEquals(5, list.get(2).get(0).intValue());
    assertEquals(6, list.get(2).get(1).intValue());
  }

  /**
   * Identical to {@link #testSplitEven()} but asserts nothing is in the origional position.
   */
  @Test
  public void testShuffling() {
    ImmutableList<Integer> origional = ImmutableList.of(1, 2, 3, 4, 5, 6);
    List<List<Integer>> split = SplitUtil.split(origional, 3, true);
    assertEquals(3, split.size());
    assertEquals(2, split.get(0).size());
    assertEquals(2, split.get(1).size());
    assertEquals(2, split.get(2).size());
    Set<Integer> resultSet = new HashSet<>();
    List<Integer> resultList = new ArrayList<>();
    for (List<Integer> l : split) {
      resultSet.addAll(l);
      resultList.addAll(l);
    }
    assertEquals(new HashSet<>(origional), resultSet);
    assertFalse(origional.equals(resultList));
  }


  @Test
  public void testSplitUneven() {
    List<List<Integer>> list = SplitUtil.split(ImmutableList.of(1, 2, 3, 4, 5, 6), 4, true);
    assertEquals(4, list.size());
    assertEquals(2, list.get(0).size());
    assertEquals(2, list.get(1).size());
    assertEquals(1, list.get(2).size());
    assertEquals(1, list.get(3).size());
  }

  @Test
  public void testSplitSparce() {
    List<List<Object>> list = SplitUtil.split(Collections.singletonList(new Object()), 3, true);
    assertEquals(1, list.size());
    assertEquals(1, list.get(0).size());
  }

  @Test
  public void testSplitEmpty() {
    List<List<Object>> list = SplitUtil.split(Collections.emptyList(), 3, true);
    assertEquals(0, list.size());
  }
}
