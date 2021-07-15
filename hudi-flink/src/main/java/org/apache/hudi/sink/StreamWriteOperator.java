/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.StreamSink;

/**
 * Operator for {@link StreamSink}.
 *
 * @param <I> The input type
 */
public class StreamWriteOperator<I>
    extends ProcessOperator<I, Object>
    implements OperatorEventHandler, BoundedOneInput {
  private final StreamWriteFunction<I, Object> sinkFunction;

  public StreamWriteOperator(Configuration conf) {
    super(new StreamWriteFunction<>(conf));
    this.sinkFunction = (StreamWriteFunction<I, Object>) getUserFunction();
  }

  @Override
  public void handleOperatorEvent(OperatorEvent event) {
    this.sinkFunction.handleOperatorEvent(event);
  }

  void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
    sinkFunction.setOperatorEventGateway(operatorEventGateway);
  }

  @Override
  public void endInput() {
    sinkFunction.endInput();
  }
}
