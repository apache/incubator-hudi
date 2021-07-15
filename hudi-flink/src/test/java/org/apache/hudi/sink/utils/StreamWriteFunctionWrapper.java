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

package org.apache.hudi.sink.utils;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.StreamWriteFunction;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.bootstrap.BootstrapFunction;
import org.apache.hudi.sink.bootstrap.IndexRecord;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.partitioner.BucketAssignFunction;
import org.apache.hudi.sink.partitioner.BucketAssignOperator;
import org.apache.hudi.sink.transform.RowDataToHoodieFunction;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorEventGateway;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A wrapper class to manipulate the {@link StreamWriteFunction} instance for testing.
 *
 * @param <I> Input type
 */
public class StreamWriteFunctionWrapper<I> {
  private final Configuration conf;

  private final IOManager ioManager;
  private final StreamingRuntimeContext runtimeContext;
  private final MockOperatorEventGateway gateway;
  private final MockOperatorCoordinatorContext coordinatorContext;
  private final StreamWriteOperatorCoordinator coordinator;
  private final MockFunctionInitializationContext functionInitializationContext;

  /**
   * Function that converts row data to HoodieRecord.
   */
  private RowDataToHoodieFunction<RowData, HoodieRecord<?>> toHoodieFunction;
  /**
   * Function that load index in state.
   */
  private BootstrapFunction<HoodieRecord<?>, HoodieRecord<?>> bootstrapFunction;
  /**
   * Function that assigns bucket ID.
   */
  private BucketAssignFunction<String, HoodieRecord<?>, HoodieRecord<?>> bucketAssignerFunction;
  /**
   * BucketAssignOperator context.
   **/
  private MockBucketAssignOperatorContext bucketAssignOperatorContext;
  /**
   * Stream write function.
   */
  private StreamWriteFunction<HoodieRecord<?>, Object> writeFunction;

  private CompactFunctionWrapper compactFunctionWrapper;

  private final boolean asyncCompaction;

  public StreamWriteFunctionWrapper(String tablePath) throws Exception {
    this(tablePath, TestConfigurations.getDefaultConf(tablePath));
  }

  public StreamWriteFunctionWrapper(String tablePath, Configuration conf) throws Exception {
    this.ioManager = new IOManagerAsync();
    MockEnvironment environment = new MockEnvironmentBuilder()
        .setTaskName("mockTask")
        .setManagedMemorySize(4 * MemoryManager.DEFAULT_PAGE_SIZE)
        .setIOManager(ioManager)
        .build();
    this.runtimeContext = new MockStreamingRuntimeContext(false, 1, 0, environment);
    this.gateway = new MockOperatorEventGateway();
    this.conf = conf;
    // one function
    this.coordinatorContext = new MockOperatorCoordinatorContext(new OperatorID(), 1);
    this.coordinator = new StreamWriteOperatorCoordinator(conf, this.coordinatorContext);
    this.compactFunctionWrapper = new CompactFunctionWrapper(this.conf);
    this.bucketAssignOperatorContext = new MockBucketAssignOperatorContext();
    this.functionInitializationContext = new MockFunctionInitializationContext();
    this.compactFunctionWrapper = new CompactFunctionWrapper(this.conf);
    this.asyncCompaction = StreamerUtil.needsAsyncCompaction(conf);
    this.bucketAssignOperatorContext = new MockBucketAssignOperatorContext();
  }

  public void openFunction() throws Exception {
    this.coordinator.start();
    this.coordinator.setExecutor(new MockCoordinatorExecutor(coordinatorContext));
    toHoodieFunction = new RowDataToHoodieFunction<>(TestConfigurations.ROW_TYPE, conf);
    toHoodieFunction.setRuntimeContext(runtimeContext);
    toHoodieFunction.open(conf);

    if (conf.getBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED)) {
      bootstrapFunction = new BootstrapFunction<>(conf);
      bootstrapFunction.setRuntimeContext(runtimeContext);
      bootstrapFunction.initializeState(this.functionInitializationContext);
      bootstrapFunction.open(conf);
    }

    bucketAssignerFunction = new BucketAssignFunction<>(conf);
    bucketAssignerFunction.setRuntimeContext(runtimeContext);
    bucketAssignerFunction.open(conf);
    bucketAssignerFunction.setContext(bucketAssignOperatorContext);
    bucketAssignerFunction.initializeState(this.functionInitializationContext);

    writeFunction = new StreamWriteFunction<>(conf);
    writeFunction.setRuntimeContext(runtimeContext);
    writeFunction.setOperatorEventGateway(gateway);
    writeFunction.initializeState(this.functionInitializationContext);
    writeFunction.open(conf);

    // handle the bootstrap event
    coordinator.handleEventFromOperator(0, getNextEvent());

    if (asyncCompaction) {
      compactFunctionWrapper.openFunction();
    }
  }

  public void invoke(I record) throws Exception {
    HoodieRecord<?> hoodieRecord = toHoodieFunction.map((RowData) record);
    HoodieRecord<?>[] hoodieRecords = new HoodieRecord[1];
    Collector<HoodieRecord<?>> collector = new Collector<HoodieRecord<?>>() {
      @Override
      public void collect(HoodieRecord<?> record) {
        hoodieRecords[0] = record;
      }

      @Override
      public void close() {

      }
    };

    if (conf.getBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED)) {
      List<HoodieRecord<?>> bootstrapRecords = new ArrayList<>();

      Collector<HoodieRecord<?>> bootstrapCollector = new Collector<HoodieRecord<?>>() {
        @Override
        public void collect(HoodieRecord<?> record) {
          if (record instanceof IndexRecord) {
            bootstrapRecords.add(record);
          }
        }

        @Override
        public void close() {

        }
      };

      bootstrapFunction.processElement(hoodieRecord, null, bootstrapCollector);
      for (HoodieRecord bootstrapRecord : bootstrapRecords) {
        bucketAssignerFunction.processElement(bootstrapRecord, null, collector);
      }

      this.bucketAssignOperatorContext.setCurrentKey(hoodieRecord.getRecordKey());
    }

    bucketAssignerFunction.processElement(hoodieRecord, null, collector);
    writeFunction.processElement(hoodieRecords[0], null, null);
  }

  public WriteMetadataEvent[] getEventBuffer() {
    return this.coordinator.getEventBuffer();
  }

  public OperatorEvent getNextEvent() {
    return this.gateway.getNextEvent();
  }

  public Map<String, List<HoodieRecord>> getDataBuffer() {
    return this.writeFunction.getDataBuffer();
  }

  @SuppressWarnings("rawtypes")
  public HoodieFlinkWriteClient getWriteClient() {
    return this.writeFunction.getWriteClient();
  }

  public void checkpointFunction(long checkpointId) throws Exception {
    // checkpoint the coordinator first
    this.coordinator.checkpointCoordinator(checkpointId, new CompletableFuture<>());
    bucketAssignerFunction.snapshotState(null);

    writeFunction.snapshotState(null);
    functionInitializationContext.getOperatorStateStore().checkpointBegin(checkpointId);
  }

  public void checkpointComplete(long checkpointId) {
    functionInitializationContext.getOperatorStateStore().checkpointSuccess(checkpointId);
    coordinator.notifyCheckpointComplete(checkpointId);
    this.bucketAssignerFunction.notifyCheckpointComplete(checkpointId);
    if (asyncCompaction) {
      try {
        compactFunctionWrapper.compact(checkpointId);
      } catch (Exception e) {
        throw new HoodieException(e);
      }
    }
  }

  public void checkpointFails(long checkpointId) {
    coordinator.notifyCheckpointAborted(checkpointId);
  }

  public void close() throws Exception {
    coordinator.close();
    ioManager.close();
  }

  public StreamWriteOperatorCoordinator getCoordinator() {
    return coordinator;
  }

  public MockOperatorCoordinatorContext getCoordinatorContext() {
    return coordinatorContext;
  }

  public void clearIndexState() {
    this.bucketAssignerFunction.clearIndexState();
    this.bucketAssignOperatorContext.clearIndexState();
  }

  public boolean isKeyInState(HoodieKey hoodieKey) {
    return this.bucketAssignOperatorContext.isKeyInState(hoodieKey.getRecordKey());
  }

  public boolean isConforming() {
    return this.writeFunction.isConfirming();
  }

  public boolean isAlreadyBootstrap() {
    return this.bootstrapFunction.isAlreadyBootstrap();
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  private static class MockBucketAssignOperatorContext implements BucketAssignOperator.Context {
    private final Set<Object> updateKeys = new HashSet<>();

    @Override
    public void setCurrentKey(Object key) {
      this.updateKeys.add(key);
    }

    public void clearIndexState() {
      this.updateKeys.clear();
    }

    public boolean isKeyInState(String key) {
      return this.updateKeys.contains(key);
    }
  }
}
