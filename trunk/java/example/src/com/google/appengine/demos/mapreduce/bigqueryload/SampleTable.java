package com.google.appengine.demos.mapreduce.bigqueryload;

import java.util.Date;
import java.util.List;

public class SampleTable {
  Long colNum;
  int col1;
  String col2;
  List<String> col3;
  String[] col4;
  SampleNestedRecord col5;
  Date col6;

  public SampleTable(Long colNum,
      int col1,
      String col2,
      List<String> col3,
      String[] col4,
      SampleNestedRecord col5,
      Date col6) {
    this.colNum = colNum;
    this.col1 = col1;
    this.col2 = col2;
    this.col3 = col3;
    this.col4 = col4;
    this.col5 = col5;
    this.col6 = col6;
  }
}
