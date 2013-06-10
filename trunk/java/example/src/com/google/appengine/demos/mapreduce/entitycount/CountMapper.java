// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.demos.mapreduce.entitycount;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.tools.mapreduce.Mapper;

import java.util.logging.Logger;

/**
 * Counts occurrences of various properties of interest.  The output key is a
 * human-readable description of the property, the value is the number of
 * occurrences.
 *
 * @author ohler@google.com (Christian Ohler)
 */
class CountMapper extends Mapper<Entity, String, Long> {
  private static final long serialVersionUID = 4973057382538885270L;

  private static final Logger log = Logger.getLogger(CountMapper.class.getName());

  public CountMapper() {
  }

  private void incrementCounter(String name, long delta) {
    getContext().getCounter(name).increment(delta);
  }

  private void emit(String outKey, long outValue) {
    //log.info("emit(" + outKey + ", " + outValue + ")");
    incrementCounter(outKey, outValue);
    getContext().emit(outKey, outValue);
  }

  private void emit1(String outKey) {
    emit(outKey, 1);
  }

  private int countChar(String s, char c) {
    int count = 0;
    for (int i = 0; i < s.length(); i++) {
      if (s.charAt(i) == c) {
        count++;
      }
    }
    return count;
  }

  @Override public void beginShard() {
    log.info("beginShard()");
    emit1("total map shard initializations");
    emit1("total map shard initializations in shard " + getContext().getShardNumber());
  }

  @Override public void beginSlice() {
    log.info("beginSlice()");
    emit1("total map slice initializations");
    emit1("total map slice initializations in shard " + getContext().getShardNumber());
  }

  @Override public void map(Entity entity) {
    //log.info("map(" + value + ")");
    emit1("total entities");
    emit1("map calls in shard " + getContext().getShardNumber());
    String name = entity.getKey().getName();
    String payload = ((Text) entity.getProperty("payload")).getValue();
    emit("total entity payload size", payload.length());
    emit("total entity key size", name.length());
    for (char c = 'a'; c <= 'z'; c++) {
      emit("occurrences of character " + c + " in payload", countChar(payload, c));
    }
    for (char c = '0'; c <= '9'; c++) {
      emit("occurrences of digit " + c + " in key", countChar(name, c));
    }
  }

  @Override public void endShard() {
    log.info("endShard()");
    emit1("total map shard terminations");
  }

  @Override public void endSlice() {
    log.info("endSlice()");
    emit1("total map slice terminations");
  }

}
