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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Add some test data for our mapper.
 *
 *
 */
public class InitServlet extends HttpServlet {
  public void doGet(HttpServletRequest req, HttpServletResponse resp) {
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    List <Entity> entities = new ArrayList<Entity>();
    for (int i = 0; i < 400; i++) {
      Entity entity = new Entity("PBFVotes");
      if (i % 2 == 0 ) {
        if (i % 4 == 0) {
          entity.setProperty("skub", "Pro");
        } else {
          entity.setProperty("skub", "Anti");
        }
      }
      entities.add(entity);
    }
    ds.put(entities);
  }
}
