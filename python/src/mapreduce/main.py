#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Main module for map-reduce implementation.

This module should be specified as a handler for mapreduce URLs in app.yaml:

  handlers:
  - url: /mapreduce/.*
    login: admin
    script: google/appengine/ext/mapreduce/main.py
"""




import wsgiref.handlers

from google.appengine.ext import webapp
from mapreduce import handlers
from mapreduce import status


def create_application():
  """Create new WSGIApplication and register all handlers.

  Returns:
    an instance of webapp.WSGIApplication with all mapreduce handlers
    registered.
  """
  return webapp.WSGIApplication([

      (r".*/worker_callback", handlers.MapperWorkerCallbackHandler),
      (r".*/controller_callback", handlers.ControllerCallbackHandler),


      (r".*/command/start_job", handlers.StartJobHandler),
      (r".*/command/cleanup_job", handlers.CleanUpJobHandler),
      (r".*/command/abort_job", handlers.AbortJobHandler),
      (r".*/command/list_configs", status.ListConfigsHandler),
      (r".*/command/list_jobs", status.ListJobsHandler),
      (r".*/command/get_job_detail", status.GetJobDetailHandler),


      (r".*/([a-zA-Z0-9]+(?:\.(?:css|js))?)", status.ResourceHandler),
  ],
  debug=True)


APP = create_application()


def main():
  wsgiref.handlers.CGIHandler().run(APP)


if __name__ == "__main__":
  main()
