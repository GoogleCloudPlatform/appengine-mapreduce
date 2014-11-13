package com.google.appengine.tools.mapreduce.testmodels;

import com.google.appengine.tools.mapreduce.BigQueryDataField;

public class SimplAnnotatedJson {
  @BigQueryDataField(name = "niceName")
  public String nameRandom;
  public String id;
  public int intField;
  /**
   * @param nameRandom
   * @param id
   * @param intField
   */
  public SimplAnnotatedJson(String nameRandom, String id, int intField) {
    this.nameRandom = nameRandom;
    this.id = id;
    this.intField = intField;
  }
}
