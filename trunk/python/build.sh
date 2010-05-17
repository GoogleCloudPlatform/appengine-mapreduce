#!/bin/bash
#
# Copyright 2010 Google Inc. All Rights Reserved.

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
