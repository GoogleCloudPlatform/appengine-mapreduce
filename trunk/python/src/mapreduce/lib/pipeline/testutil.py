#!/usr/bin/env python
#
# Copyright 2009 Google Inc.
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

"""Test utilities for the Google App Engine Pipeline API."""

# Code originally from:
#   http://code.google.com/p/pubsubhubbub/source/browse/trunk/hub/testutil.py

import logging
import os
import sys
import tempfile


TEST_APP_ID = 'my-app-id'
TEST_VERSION_ID = 'my-version.1234'

# Assign the application ID up front here so we can create db.Key instances
# before doing any other test setup.
os.environ['APPLICATION_ID'] = TEST_APP_ID
os.environ['CURRENT_VERSION_ID'] = TEST_VERSION_ID
os.environ['HTTP_HOST'] = '%s.appspot.com' % TEST_APP_ID


def fix_path():
  """Finds the google_appengine directory and fixes Python imports to use it."""
  all_paths = os.environ.get('PATH').split(os.pathsep)
  for path_dir in all_paths:
    dev_appserver_path = os.path.join(path_dir, 'dev_appserver.py')
    if os.path.exists(dev_appserver_path):
      google_appengine = os.path.dirname(os.path.realpath(dev_appserver_path))
      sys.path.append(google_appengine)
      # Use the next import will fix up sys.path even further to bring in
      # any dependent lib directories that the SDK needs.
      dev_appserver = __import__('dev_appserver')
      sys.path.extend(dev_appserver.EXTRA_PATHS)
      return


def setup_for_testing(require_indexes=True, define_queues=[]):
  """Sets up the stubs for testing.

  Args:
    require_indexes: True if indexes should be required for all indexes.
    define_queues: Additional queues that should be available.
  """
  from google.appengine.api import apiproxy_stub_map
  from google.appengine.api import memcache
  from google.appengine.api import queueinfo
  from google.appengine.tools import dev_appserver
  from google.appengine.tools import dev_appserver_index
  before_level = logging.getLogger().getEffectiveLevel()
  try:
    logging.getLogger().setLevel(100)
    root_path = os.path.realpath(os.path.dirname(__file__))
    dev_appserver.SetupStubs(
        TEST_APP_ID,
        root_path=root_path,
        login_url='',
        datastore_path=tempfile.mktemp(suffix='datastore_stub'),
        history_path=tempfile.mktemp(suffix='datastore_history'),
        blobstore_path=tempfile.mktemp(suffix='blobstore_stub'),
        require_indexes=require_indexes,
        clear_datastore=False)
    dev_appserver_index.SetupIndexes(TEST_APP_ID, root_path)
    # Actually need to flush, even though we've reallocated. Maybe because the
    # memcache stub's cache is at the module level, not the API stub?
    memcache.flush_all()
  finally:
    logging.getLogger().setLevel(before_level)

  taskqueue_stub = apiproxy_stub_map.apiproxy.GetStub('taskqueue')
  taskqueue_stub.queue_yaml_parser = (
      lambda x: queueinfo.LoadSingleQueue(
          'queue:\n- name: default\n  rate: 1/s\n' +
          '\n'.join('- name: %s\n  rate: 1/s' % name
                    for name in define_queues)))
