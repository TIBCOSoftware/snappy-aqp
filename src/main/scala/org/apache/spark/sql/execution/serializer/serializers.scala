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
package org.apache.spark.sql.execution.serializer

import org.apache.spark.sql.execution.streamsummary.StreamSummaryAggregation
import org.apache.spark.sql.execution.TopKHokusai
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input

class TopkHokusaiKryoSerializer extends Serializer[TopKHokusai[_]] {

  override def write(kryo: Kryo, output: Output, obj: TopKHokusai[_]) {
    TopKHokusai.write(kryo, output, obj)
  }

  override def read(kryo: Kryo, input: Input, typee: Class[TopKHokusai[_]]): TopKHokusai[_] = {
    TopKHokusai.read(kryo, input)
  }
}

class StreamSummaryAggregationKryoSerializer extends Serializer[StreamSummaryAggregation[_]] {

  override def write(kryo: Kryo, output: Output, obj: StreamSummaryAggregation[_]) {
    StreamSummaryAggregation.write(kryo, output, obj)
  }

  override def read(kryo: Kryo, input: Input,
    typee: Class[StreamSummaryAggregation[_]]): StreamSummaryAggregation[_] = {
    StreamSummaryAggregation.read(kryo, input)
  }
}
