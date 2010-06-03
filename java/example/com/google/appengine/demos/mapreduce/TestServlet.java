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

package com.google.appengine.demos.mapreduce;

import com.google.appengine.tools.mapreduce.ConfigurationXmlUtil;
import com.google.appengine.tools.mapreduce.DatastoreInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * A sample app using the programmatic interface to start a MapReduce.
 *
 * @author frew@google.com (Fred Wulff)
 */
public class TestServlet extends HttpServlet {

  public TestServlet() {
  }

  private String generateHtml(String configXml) {
    return "<html>"
        + "<body>"
        + "<form action=\"/mapreduce/start\" method=\"POST\">"
        + "<textarea name=\"configuration\" rows=20 cols=80>"
        + configXml
        + "</textarea>"
        + "<input type=\"submit\" value=\"Start\">"
        + "</form>";
  }

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) {
    // Generate the configuration programmatically
    Configuration conf = new Configuration(false);
    try {
      conf.setClass("mapreduce.map.class", TestMapper.class, Mapper.class);
      conf.setClass("mapreduce.inputformat.class", DatastoreInputFormat.class, InputFormat.class);
      conf.set(DatastoreInputFormat.ENTITY_KIND_KEY, "PBFVotes");

      // Render it as an HTML form so that the user can edit it.
      String html = generateHtml(
          ConfigurationXmlUtil.convertConfigurationToXml(conf));
      PrintWriter pw = new PrintWriter(resp.getOutputStream());
      pw.println(html);
      pw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
