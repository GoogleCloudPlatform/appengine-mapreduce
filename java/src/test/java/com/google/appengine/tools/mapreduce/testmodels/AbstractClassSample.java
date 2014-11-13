package com.google.appengine.tools.mapreduce.testmodels;

public abstract class AbstractClassSample {
  int id;
  String name;

  /**
   * @param id
   * @param name
   */
  public AbstractClassSample(int id, String name) {
    this.id = id;
    this.name = name;
  }
}
