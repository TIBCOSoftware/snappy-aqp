#!/usr/bin/env bash

##################
# helper functions
##################
is_file_generated(){
  file_path=$1
  if [ -e "$file_path" ];then
    file_name=`basename $file_path`
    echo "SUCCESS: file $file_name generated"
  else
    echo "ERROR: file_path $file_path not generated"
    exit 1
  fi
}

rm_if_exists(){
  file_path=$1
  if [ -e "$file_path" ];then
    echo "WARNING: deleting $file_path"
    rm $file_path
  fi
}

function absPath() {
  perl -MCwd -le 'print Cwd::abs_path(shift)' "$1"
}

if [ "$JAVA_HOME" == "" ]; then
  echo "ERROR: JAVA_HOME is not set"
  exit 1;
fi

CURR_DIR=`pwd`
script_real_path=`absPath $0`
# script home is assumed to be
# <SNAPPY_HOME>/aqp/src/main/cpp/io/snappydata
script_home=`dirname $script_real_path`
aqp_home=$script_home/../../../../..
aqp_lib=$aqp_home/lib

# generate so files
cd $script_home

case "$(uname -s)" in
  Darwin)
    os_name="darwin"
    rm_if_exists $aqp_lib/libnative64.dylib
    rm_if_exists $aqp_lib/libnative.dylib
    clang -m64 -O3 -fPIC -DPIC -D_REENTRANT -I $JAVA_HOME/include -I $JAVA_HOME/include/$os_name -dynamiclib DataOptimizations.c -o $aqp_lib/libnative64.dylib
    is_file_generated $aqp_lib/libnative64.dylib
    clang -m32 -O3 -fPIC -DPIC -D_REENTRANT -I $JAVA_HOME/include -I $JAVA_HOME/include/$os_name -dynamiclib DataOptimizations.c -o $aqp_lib/libnative.dylib
    is_file_generated $aqp_lib/libnative.dylib

    chmod -x $aqp_lib/*.dylib
  ;;
  Linux)
    os_name="linux"
    rm_if_exists $aqp_lib/libnative64.so
    rm_if_exists $aqp_lib/libnative.so
    gcc -m64 -O3 -fPIC -DPIC -D_REENTRANT -I $JAVA_HOME/include -I $JAVA_HOME/include/$os_name -shared DataOptimizations.c -o $aqp_lib/libnative64.so
    is_file_generated $aqp_lib/libnative64.so
    gcc -m32 -O3 -fPIC -DPIC -D_REENTRANT -I $JAVA_HOME/include -I $JAVA_HOME/include/$os_name -shared DataOptimizations.c -o $aqp_lib/libnative.so
    is_file_generated $aqp_lib/libnative.so

    chmod -x $aqp_lib/*.so
  ;;
  *)
    echo "ERROR: kernel not recognized"
    exit 1
  ;;
esac

# go back to initial working dir
cd $CURR_DIR
