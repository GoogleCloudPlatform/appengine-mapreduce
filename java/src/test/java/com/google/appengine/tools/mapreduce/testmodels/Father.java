package com.google.appengine.tools.mapreduce.testmodels;

import java.util.List;

/**
 * Test class for testing BigQueryDataMarshaller
 */
public class Father {
  public boolean married;
  public String name;
  public List<Child> sons;
  /**
   * @param married
   * @param name
   * @param sons
   */
  public Father(boolean married, String name, List<Child> sons) {
    this.married = married;
    this.name = name;
    this.sons = sons;
  }
  
  
}
