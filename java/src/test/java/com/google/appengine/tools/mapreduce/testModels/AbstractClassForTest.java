package com.google.appengine.tools.mapreduce.testModels;

public abstract class AbstractClassForTest {
  int id;
  String name;

  /**
   * @param id
   * @param name
   */
  public AbstractClassForTest(int id, String name) {
    this.id = id;
    this.name = name;
  }
}
