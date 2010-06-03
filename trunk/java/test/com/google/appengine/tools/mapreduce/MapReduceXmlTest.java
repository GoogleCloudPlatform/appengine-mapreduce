/*
 * Copyright 2010 Google Inc.
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

package com.google.appengine.tools.mapreduce;

import com.google.common.collect.Sets;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;

/**
 * Tests the {@link MapReduceXml} class.
 * 
 * @author frew@google.com (Fred Wulff)
 */
public class MapReduceXmlTest extends TestCase {
  // Also used by MapReduceServletTest
  static final String SAMPLE_CONFIGURATION_XML =
        "<configurations>"
      + "  <configuration name=\"Foo\" />"
      + "  <configuration name=\"Bar\">"
      + "    <property>"
      + "      <name>Baz</name>"
      + "      <value template=\"optional\">2</value>"
      + "    </property>"
      + "  </configuration>"
      + "</configurations>";
  
  public InputStream inputStreamFromString(String s) {
    try {
      return new ByteArrayInputStream(s.getBytes("UTF8"));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Huh. JDK doesn't support UTF8");
    }
  }
  
  public void testConstructor() {
    MapReduceXml mrXml = new MapReduceXml(inputStreamFromString(SAMPLE_CONFIGURATION_XML));
    assertEquals(Sets.newHashSet(mrXml.getConfigurationNames()), Sets.newHashSet("Foo", "Bar"));
  }
  
  public void testTemplateIsIntact() {
    MapReduceXml mrXml = new MapReduceXml(inputStreamFromString(SAMPLE_CONFIGURATION_XML));
    String templateString = mrXml.getTemplateAsXmlString("Bar");
    ConfigurationTemplatePreprocessor preprocessor = 
        new ConfigurationTemplatePreprocessor(templateString);
    String configString = preprocessor.preprocess(new HashMap<String, String>());
    Configuration conf = ConfigurationXmlUtil.getConfigurationFromXml(configString);
    assertEquals(2, conf.getInt("Baz", 0));
  }
}
