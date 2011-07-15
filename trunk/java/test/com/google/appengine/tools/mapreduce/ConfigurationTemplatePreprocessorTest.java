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

import static java.util.Collections.singletonMap;

import com.google.common.collect.Sets;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;

/**
 * Tests {@link ConfigurationTemplatePreprocessor}
 *
 */
public class ConfigurationTemplatePreprocessorTest extends TestCase {
  private static final String NONTEMPLATE_PROPERTY =
      "<property><name>binky</name><value>winky</value></property>";

  private static final String SIMPLE_TEMPLATE_PROPERTY =
      "<property><name>foo</name><value template=\"required\" /></property>";

  // SIMPLE_TEMPLATE_PROPERTY with "zzz" as value
  private static final String SIMPLE_ZZZ_PROPERTY =
      "<property><name>foo</name><value>zzz</value></property>";

  private static final String HUMAN_TEMPLATE_PROPERTY =
      "<property><name human=\"Foo'bar\">bar</name><value template=\"required\" /></property>";

  // HUMAN_TEMPLATE_PROPERTY with "zzz" as value
  private static final String HUMAN_ZZZ_PROPERTY =
      "<property><name>bar</name><value>zzz</value></property>";

  private static final String DEFAULT_TEMPLATE_PROPERTY =
      "<property><name>baz</name><value template=\"optional\">Whee</value></property>";

  // DEFAULT_TEMPLATE_PROPERTY with "Whee" as value
  private static final String DEFAULT_WHEE_PROPERTY =
      "<property><name>baz</name><value>Whee</value></property>";

  private static final String EMPTY_DEFAULT_TEMPLATE_PROPERTY =
      "<property><name>baz</name><value template=\"optional\" /></property>";

  private static final String EMPTY_EXPANDED_PROPERTY =
    "<property><name>baz</name><value></value></property>";

  public ConfigurationTemplatePreprocessor getPreprocessorForProperties(String... properties) {
    StringBuffer buf = new StringBuffer();
    for (String property : properties) {
      buf.append(property);
    }
    return new ConfigurationTemplatePreprocessor(
        "<configuration>" + buf.toString() + "</configuration>");
  }

  public String getExpectedStringForProperties(String... properties) {
    StringBuffer buf = new StringBuffer();
    for (String property : properties) {
      buf.append(property);
    }
    return "<configuration>" + buf.toString() + "</configuration>";
  }

  /**
   * Compares configurations given their XML strings.
   */
  private void assertConfigurationsEqual(String expectedConfXml, String actualConfXml) {
    Configuration expectedConf = ConfigurationXmlUtil.getConfigurationFromXml(expectedConfXml);
    Configuration actualConf = ConfigurationXmlUtil.getConfigurationFromXml(actualConfXml);

    assertEquals(Sets.newHashSet(expectedConf), Sets.newHashSet(actualConf));
  }

  public void testPreprocess_nontemplate() {
    ConfigurationTemplatePreprocessor proc = getPreprocessorForProperties(NONTEMPLATE_PROPERTY);
    String outputString = proc.preprocess(new HashMap<String, String>());

    assertConfigurationsEqual(getExpectedStringForProperties(NONTEMPLATE_PROPERTY), outputString);
  }

  public void testPreprocess_defaultMissing() {
    ConfigurationTemplatePreprocessor proc = getPreprocessorForProperties(SIMPLE_TEMPLATE_PROPERTY);
    try {
      proc.preprocess(new HashMap<String, String>());
      fail("Should have caught missing property");
    } catch (MissingTemplateParameterException expected) {
    }
  }

  public void testPreprocess_nontemplateExtraParam() {
    ConfigurationTemplatePreprocessor proc = getPreprocessorForProperties(NONTEMPLATE_PROPERTY);
    try {
      proc.preprocess(singletonMap("foo", "bar"));
      fail("Should have caught extra property");
    } catch (UnexpectedTemplateParameterException expected) {
    }
  }

  public void testPreprocess_simpleParam() {
    ConfigurationTemplatePreprocessor proc = getPreprocessorForProperties(SIMPLE_TEMPLATE_PROPERTY);
    assertEquals(
        "{foo={humanName=foo, name=foo}}", proc.getMetadataMap().toString());
    assertConfigurationsEqual(getExpectedStringForProperties(SIMPLE_ZZZ_PROPERTY),
        proc.preprocess(singletonMap("foo", "zzz")));
  }

  public void testPreprocess_humanParam() {
    ConfigurationTemplatePreprocessor proc = getPreprocessorForProperties(HUMAN_TEMPLATE_PROPERTY);
    assertEquals(
        "{bar={humanName=Foo'bar, name=bar}}", proc.getMetadataMap().toString());
    assertConfigurationsEqual(getExpectedStringForProperties(HUMAN_ZZZ_PROPERTY),
        proc.preprocess(singletonMap("bar", "zzz")));
  }

  public void testPreprocess_defaultParam() {
    ConfigurationTemplatePreprocessor proc = getPreprocessorForProperties(
        DEFAULT_TEMPLATE_PROPERTY);

    assertConfigurationsEqual(getExpectedStringForProperties(DEFAULT_WHEE_PROPERTY),
        proc.preprocess(new HashMap<String, String>()));
  }

  public void testPreprocess_emptyDefaultParam() {
    ConfigurationTemplatePreprocessor proc = getPreprocessorForProperties(
        EMPTY_DEFAULT_TEMPLATE_PROPERTY);

    assertConfigurationsEqual(getExpectedStringForProperties(EMPTY_EXPANDED_PROPERTY),
        proc.preprocess(new HashMap<String, String>()));
  }

  public void testPreprocess_cantCallTwice() {
    ConfigurationTemplatePreprocessor proc = getPreprocessorForProperties(
        DEFAULT_TEMPLATE_PROPERTY);
    try {
      proc.preprocess(new HashMap<String, String>());
      proc.preprocess(new HashMap<String, String>());
      fail("Didn't throw IllegalStateException on second preprocess call.");
    } catch (IllegalStateException expected) {
    }
  }
}
