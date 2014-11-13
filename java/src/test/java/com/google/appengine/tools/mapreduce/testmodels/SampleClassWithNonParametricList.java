package com.google.appengine.tools.mapreduce.testmodels;

import java.util.List;

public class SampleClassWithNonParametricList {
  @SuppressWarnings("rawtypes")
  List l;

  /**
   * @param l
   */
  @SuppressWarnings("rawtypes")
  public SampleClassWithNonParametricList(List l) {
    this.l = l;
  }
}
