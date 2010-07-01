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

  export PYTHONPATH="\
$APPENGINE_LIB:\
$APPENGINE_LIB/lib/webob:\
$APPENGINE_LIB/lib/yaml/lib:\
$APPENGINE_LIB/lib/django:\
$dir/src:\
$dir/test:\
"

  echo "Using PYTHONPATH=$PYTHONPATH"
  for t in $(find "$dir/test" -name "*test.py"); do
    python $t
  done
}

build_demo () {
  [ ! -d "$dir/demo/mapreduce" ] && ln -s "$dir/src/mapreduce" "$dir/demo"
}

run_demo () {
  build_demo
  dev_appserver.py "$dir/demo"
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
