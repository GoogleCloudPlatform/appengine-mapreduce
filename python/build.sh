#!/bin/bash
#
# Copyright 2010 Google Inc.
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

dir=`dirname $0`

test () {
  if [ -z $APPENGINE_LIB ]; then
    echo "APPENGINE_LIB environment variable shoud be defined and should point to appengine sdk folder"
    exit 1
  fi

  export PYTHONPATH="${PYTHONPATH}:\
$APPENGINE_LIB:\
$APPENGINE_LIB/lib/fancy_urllib:\
$APPENGINE_LIB/lib/webob-1.1.1:\
$APPENGINE_LIB/lib/yaml/lib:\
$dir/src:\
$dir/test:\
"
  fetch_dependencies
  echo "Using PYTHONPATH=$PYTHONPATH"
  exit_status=0
  for t in $(find "$dir/test" -name "*test.py" | grep -Ev "/test/mapreduce/api/|test/mapreduce/module_test.py|test/mapreduce/webapp2_test.py"); do
    if python $t
    then
      echo "PASSED"
    else
      echo "FAILED"
      ((exit_status++))
    fi
  done

  echo "----------------------------------------------------------------------"
  if [ $exit_status -ne 0 ];
  then
    echo "FAILED $exit_status tests"
  else
    echo "PASSED all tests"
  fi
  exit $exit_status
}

build_demo () {
  fetch_dependencies
  [ ! -d "$dir/demo/mapreduce" ] && ln -s "$dir/src/mapreduce" "$dir/demo"
}

run_demo () {
  build_demo
  dev_appserver.py "$dir/demo"
}

fetch_dependencies() {
  if [ ! `which pip` ]
  then
    echo "pip not found. pip is required to install dependencies."
    exit 1;
  fi
  # This may fail due to https://github.com/pypa/pip/issues/1356
  pip install --exists-action=s -r $dir/src/requirements.txt -t $dir/src/ || exit 1
  pip install --exists-action=s -r $dir/src/requirements.txt -t $dir/demo/ || exit 1
}

case "$1" in
  test)
    test
    ;;
  build_demo)
    build_demo
    ;;
  run_demo)
    run_demo
    ;;
  *)
    echo $"Usage: $0 {test|build_demo|run_demo}"
    exit 1
esac
