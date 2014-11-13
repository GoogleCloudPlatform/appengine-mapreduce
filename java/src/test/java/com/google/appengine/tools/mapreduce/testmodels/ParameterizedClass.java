package com.google.appengine.tools.mapreduce.testmodels;

public class ParameterizedClass<T> {
  T id;
  /**
   * @param id
   */
  public ParameterizedClass(T id) {
    this.id = id;
  }
}
