package com.google.appengine.tools.mapreduce.testmodels;

import com.google.appengine.tools.mapreduce.BigQueryIgnore;

/**
 * Test class for BigQueryMarshaller testing
 */

public class Man {
  @BigQueryIgnore
  public int id;
  public String name;
  public String gender;

  /**
   * @param id
   * @param name
   * @param gender
   */
  public Man(int id, String name, String gender) {
    this.id = id;
    this.name = name;
    this.gender = gender;
  }
}
