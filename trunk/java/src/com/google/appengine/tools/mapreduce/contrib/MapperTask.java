/*
 * Copyright 2011 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.mapreduce.contrib;

import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Method;
import com.google.appengine.tools.mapreduce.ConfigurationXmlUtil;
import com.google.appengine.tools.mapreduce.DatastoreInputFormat;
import com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Map;
import java.util.Map.Entry;

/**
 * Utility class to construct a Mapper task.
 *
 */
public class MapperTask {
  private String taskName;
  private String entityKind;
  private Class<?> mapperClass;
  private Map<String, String> params = Maps.newHashMap();

  public MapperTask withName(String taskName) {
    this.taskName = taskName;
    return this;
  }

  public MapperTask onEntity(String entityKind) {
    this.entityKind = entityKind;
    return this;
  }

  public MapperTask usingMapper(Class<?> mapperClass) {
    this.mapperClass = mapperClass;
    return this;
  }

  public MapperTask addParam(String key, String value) {
    this.params.put(key, value);
    return this;
  }

  public TaskOptions build() {
    return TaskOptions.Builder.withUrl("/mapreduce/start").method(Method.POST)
      .header("X-Requested-With", "XMLHttpRequest").param("name", taskName)
      .param("configuration", createConfiguration(entityKind, mapperClass, params));
  }

  public static String createConfiguration(String entityKind, Class<?> mapperClass,
      Map<String, String> additionalParams) {
    Configuration conf = new Configuration(false);
    conf.setClass("mapreduce.map.class", mapperClass, Mapper.class);
    conf.setClass("mapreduce.inputformat.class", DatastoreInputFormat.class, InputFormat.class);
    conf.set(DatastoreInputFormat.ENTITY_KIND_KEY, entityKind);
    for (Entry<String, String> entry : additionalParams.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    return ConfigurationXmlUtil.convertConfigurationToXml(conf);
  }
}
