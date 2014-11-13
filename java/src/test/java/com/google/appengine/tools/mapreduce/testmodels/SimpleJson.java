package com.google.appengine.tools.mapreduce.testmodels;

import com.google.appengine.tools.mapreduce.BigQueryDataField;
import com.google.appengine.tools.mapreduce.BigQueryFieldMode;

/**
 * Simple class for testing
 */
public class SimpleJson {
  @BigQueryDataField(mode = BigQueryFieldMode.REQUIRED)
    public String name;
    public int id;
    /**
     * @param name
     * @param id
     */
    public SimpleJson(String name, int id) {
      this.name = name;
      this.id = id;
    }
}
