package com.google.appengine.tools.mapreduce.testModels;

import com.google.appengine.tools.mapreduce.BigQueryDataField;


public class SimplAnnotatedJson {
  @BigQueryDataField(name="niceName")
  public String name_random;
  public String id;
  public int intField;
  /**
   * @param name_random
   * @param id
   * @param intField
   */
  public SimplAnnotatedJson(String name_random, String id, int intField) {
    this.name_random = name_random;
    this.id = id;
    this.intField = intField;
  }
  
  
}
