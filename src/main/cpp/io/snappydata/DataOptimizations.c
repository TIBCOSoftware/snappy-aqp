/*
 * Copyright (c) 2017-2021 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

#include <jni.h>
#include <string.h>

/*
 * Class:     org_apache_spark_unsafe_Native
 * Method:    arrayEquals
 * Signature: (JJJ)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_spark_unsafe_Native_arrayEquals(
    JNIEnv* env, jclass c, jlong leftAddress, jlong rightAddress, jlong size) {
  return memcmp((const void*)leftAddress, (const void*)rightAddress, (size_t)size) == 0;
}

/*
 * Class:     org_apache_spark_unsafe_Native
 * Method:    compareString
 * Signature: (JJJ)Z
 */
JNIEXPORT jint JNICALL Java_org_apache_spark_unsafe_Native_compareString(
    JNIEnv* env, jclass c, jlong leftAddress, jlong rightAddress, jlong size) {
  return memcmp((const void*)leftAddress, (const void*)rightAddress, (size_t)size);
}

/*
 * Class:     org_apache_spark_unsafe_Native
 * Method:    containsString
 * Signature: (JJJI)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_spark_unsafe_Native_containsString(
    JNIEnv* env, jclass c, jlong source, jint sourceSize, jlong dest, jint destSize) {
  if (destSize > 0) {
    const char* sourceFirst = (const char*)source;
    const char* sourceLast = sourceFirst + sourceSize;
    const char* destFirst = (const char*)dest;
    const char first = *destFirst;
    int remaining = sourceSize - destSize + 1;

    while (remaining > 0) {
      // find the next occurence of first byte
      if (!(sourceFirst = (const char*)memchr(sourceFirst, first, remaining))) {
        return JNI_FALSE;
      }
      // compare full string including first byte for aligned comparison
      if (memcmp(sourceFirst, destFirst, destSize) == 0) {
        return JNI_TRUE;
      }
      remaining = sourceLast - (++sourceFirst) - destSize + 1;
    }
  }
  return JNI_FALSE;
}
