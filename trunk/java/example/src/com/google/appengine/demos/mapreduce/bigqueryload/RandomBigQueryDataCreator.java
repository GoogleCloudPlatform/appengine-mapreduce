package com.google.appengine.demos.mapreduce.bigqueryload;

import com.google.appengine.tools.mapreduce.MapOnlyMapper;
import com.google.common.collect.Lists;

import java.util.Date;
import java.util.Random;

public class RandomBigQueryDataCreator extends MapOnlyMapper<Long, SampleTable> {

  private static final long serialVersionUID = -4247519870584497230L;
  private static Random r;

  @Override
  public void map(Long value) {
    SampleTable toWrite = getSampleTableData(value);

    emit(toWrite);
  }

  public static SampleTable getSampleTableData(Long value) {
    r = new Random(value);
    SampleTable toWrite = new SampleTable(value,
        randomInt(),
        String.format("colvalue %d", randomInt()),
        Lists.newArrayList(String.format("column value %d", randomInt()),
            String.format("colvalue %d", randomInt())),
        new String[] {String.format("column value %d", randomInt()),
            String.format("column value %d", randomInt())},
        new SampleNestedRecord(randomInt(), String.format("column value %d", randomInt())),
        new Date(randomInt()));
    return toWrite;
  }

  private static int randomInt() {
    return r.nextInt();
  }

}
