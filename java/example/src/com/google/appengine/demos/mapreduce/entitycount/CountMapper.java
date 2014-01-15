package com.google.appengine.demos.mapreduce.entitycount;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.tools.mapreduce.Mapper;

import java.util.HashMap;
import java.util.Map.Entry;

/**
 * Counts occurrences of characters in the key and the "payload" property of datastore entities.
 * The output key is a human-readable description of the property, the value is the number of
 * occurrences.
 *
 * @author ohler@google.com (Christian Ohler)
 */
class CountMapper extends Mapper<Entity, String, Long> {

  private static final long serialVersionUID = 4973057382538885270L;

  private void incrementCounter(String name, long delta) {
    getContext().getCounter(name).increment(delta);
  }

  private void emitCharacterCounts(String s) {
    HashMap<Character, Integer> counts = new HashMap<>();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      Integer count = counts.get(c);
      if (count == null) {
        counts.put(c, 1);
      } else {
        counts.put(c, count + 1);
      }
    }
    for (Entry<Character, Integer> kv : counts.entrySet()) {
      emit(String.valueOf(kv.getKey()), Long.valueOf(kv.getValue()));
    }
  }

  @Override
  public void map(Entity entity) {
    incrementCounter("total entities", 1);
    incrementCounter("map calls in shard " + getContext().getShardNumber(), 1);

    String name = entity.getKey().getName();
    if (name != null) {
      incrementCounter("total entity key size", name.length());
      emitCharacterCounts(name);
    }

   Text property = (Text) entity.getProperty("payload");
   if (property != null) {
     incrementCounter("total entity payload size", property.getValue().length());
     emitCharacterCounts(property.getValue());
   }
  }
}
