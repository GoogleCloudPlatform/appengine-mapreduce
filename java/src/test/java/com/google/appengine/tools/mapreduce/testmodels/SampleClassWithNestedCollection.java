package com.google.appengine.tools.mapreduce.testmodels;

import java.util.List;

public class SampleClassWithNestedCollection {
  List<List<String>> ll;

  /**
   * @param ll
   */
  public SampleClassWithNestedCollection(List<List<String>> ll) {
    this.ll = ll;
  }
  
  
}
