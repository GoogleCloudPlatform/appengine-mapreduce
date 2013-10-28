/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.mapreduce.impl.handlers.MapReduceServletImpl;
import com.google.appengine.tools.mapreduce.impl.handlers.RejectRequestException;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet for all MapReduce API related functions.
 *
 * This should be specified as the handler for MapReduce URLs in web.xml.
 * For instance:
 * <pre>
 * {@code
 * <servlet>
 *   <servlet-name>mapreduce</servlet-name>
 *   <servlet-class>com.google.appengine.tools.mapreduce.MapReduceServlet</servlet-class>
 * </servlet>
 * <servlet-mapping>
 *   <servlet-name>mapreduce</servlet-name>
 *   <url-pattern>/mapreduce/*</url-pattern>
 * </servlet-mapping>
 * }
 *
 * Generally you'll want this handler to be protected by an admin security constraint
 * (see <a
 * href="http://code.google.com/appengine/docs/java/config/webxml.html#Security_and_Authentication">
 * Security and Authentication</a>)
 * for more details.
 * </pre>
 *
 */
public class MapReduceServlet extends HttpServlet {
  private static final long serialVersionUID = 899229972193207939L;
  private static final Logger log = Logger.getLogger(MapReduceServlet.class.getName());

  private static int REJECT_REQUEST_STATUSCODE = 429; // See rfc6585

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    try {
      MapReduceServletImpl.doPost(req, resp);
    } catch (RejectRequestException e) {
      handleRejectedRequest(resp, e);
    }
  }

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    try {
      MapReduceServletImpl.doGet(req, resp);
    } catch (RejectRequestException e) {
      handleRejectedRequest(resp, e);
    }
  }

  private static void handleRejectedRequest(HttpServletResponse resp, RejectRequestException e) {
    resp.addIntHeader("Retry-After", 0);
    resp.setStatus(REJECT_REQUEST_STATUSCODE);
    log.log(Level.INFO, "Rejecting request: " + e.getLocalizedMessage());
  }


}
