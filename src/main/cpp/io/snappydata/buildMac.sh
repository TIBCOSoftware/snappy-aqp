#!/bin/sh

rm -f *.dylib

if [ -z "${JAVA_HOME}" ]; then
  export JAVA_HOME="`/usr/libexec/java_home`"
fi

clang -m64 -O3 -fPIC -DPIC -D_REENTRANT -I $JAVA_HOME/include -I $JAVA_HOME/include/darwin -dynamiclib DataOptimizations.c -o libnative64.dylib
clang -m32 -O3 -fPIC -DPIC -D_REENTRANT -I $JAVA_HOME/include -I $JAVA_HOME/include/darwin -dynamiclib DataOptimizations.c -o libnative.dylib
