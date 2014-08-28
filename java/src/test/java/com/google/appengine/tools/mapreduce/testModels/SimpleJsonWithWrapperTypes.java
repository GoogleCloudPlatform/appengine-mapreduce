package com.google.appengine.tools.mapreduce.testModels;


public class SimpleJsonWithWrapperTypes {
  Integer id;
  String name;
  Float value;
  /**
   * @param id
   * @param name
   * @param value
   */
  public SimpleJsonWithWrapperTypes(Integer id, String name, Float value) {
    this.id = id;
    this.name = name;
    this.value = value;
  }
}
