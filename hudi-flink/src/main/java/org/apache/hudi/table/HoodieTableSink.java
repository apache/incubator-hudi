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

package org.apache.hudi.table;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.CleanFunction;
import org.apache.hudi.sink.StreamWriteOperatorFactory;
import org.apache.hudi.sink.bootstrap.BootstrapFunction;
import org.apache.hudi.sink.compact.CompactFunction;
import org.apache.hudi.sink.compact.CompactionCommitEvent;
import org.apache.hudi.sink.compact.CompactionCommitSink;
import org.apache.hudi.sink.compact.CompactionPlanEvent;
import org.apache.hudi.sink.compact.CompactionPlanOperator;
import org.apache.hudi.sink.partitioner.BucketAssignFunction;
import org.apache.hudi.sink.partitioner.BucketAssignOperator;
import org.apache.hudi.sink.transform.RowDataToHoodieFunction;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.Map;

/**
 * Hoodie table sink.
 */
public class HoodieTableSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {

  private final Configuration conf;
  private final TableSchema schema;
  private boolean overwrite = false;

  public HoodieTableSink(Configuration conf, TableSchema schema) {
    this.conf = conf;
    this.schema = schema;
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    return (DataStreamSinkProvider) dataStream -> {
      // Read from kafka source
      RowType rowType = (RowType) schema.toRowDataType().notNull().getLogicalType();
      long ckpTimeout = dataStream.getExecutionEnvironment()
          .getCheckpointConfig().getCheckpointTimeout();
      conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);
      StreamWriteOperatorFactory<HoodieRecord> operatorFactory = new StreamWriteOperatorFactory<>(conf);

      DataStream<HoodieRecord> hoodieDataStream = dataStream
          .map(new RowDataToHoodieFunction<>(rowType, conf), TypeInformation.of(HoodieRecord.class));

      // TODO: This is a very time-consuming operation, will optimization
      if (conf.getBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED)) {
        hoodieDataStream = hoodieDataStream.rebalance()
            .transform(
                "index_bootstrap",
                TypeInformation.of(HoodieRecord.class),
                new ProcessOperator<>(new BootstrapFunction<>(conf)))
            .setParallelism(conf.getInteger(FlinkOptions.INDEX_BOOTSTRAP_TASKS))
            .uid("uid_index_bootstrap_" + conf.getString(FlinkOptions.TABLE_NAME));
      }

      DataStream<HoodieRecord> bucketAssignStream = hoodieDataStream
          // Key-by record key, to avoid multiple subtasks write to a bucket at the same time
          .keyBy(HoodieRecord::getRecordKey)
          .transform(
              "bucket_assigner",
              TypeInformation.of(HoodieRecord.class),
              new BucketAssignOperator<>(new BucketAssignFunction<>(conf)))
          .setParallelism(conf.getInteger(FlinkOptions.BUCKET_ASSIGN_TASKS))
          .uid("uid_bucket_assigner_" + conf.getString(FlinkOptions.TABLE_NAME));

      // shuffle by fileId(bucket id)
      if (!WriteOperationType.fromValue(conf.get(FlinkOptions.OPERATION)).equals(WriteOperationType.INSERT)) {
        bucketAssignStream = bucketAssignStream.keyBy(record -> record.getCurrentLocation().getFileId());
      }

      DataStream<Object> pipeline = bucketAssignStream
          .transform("hoodie_stream_write", TypeInformation.of(Object.class), operatorFactory)
          .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS));
      if (StreamerUtil.needsAsyncCompaction(conf)) {
        return pipeline.transform("compact_plan_generate",
            TypeInformation.of(CompactionPlanEvent.class),
            new CompactionPlanOperator(conf))
            .setParallelism(1) // plan generate must be singleton
            .rebalance()
            .transform("compact_task",
                TypeInformation.of(CompactionCommitEvent.class),
                new ProcessOperator<>(new CompactFunction(conf)))
            .setParallelism(conf.getInteger(FlinkOptions.COMPACTION_TASKS))
            .addSink(new CompactionCommitSink(conf))
            .name("compact_commit")
            .setParallelism(1); // compaction commit should be singleton
      } else {
        return pipeline.addSink(new CleanFunction<>(conf))
            .setParallelism(1)
            .name("clean_commits");
      }
    };
  }

  @VisibleForTesting
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
    // ignore RowKind.UPDATE_BEFORE
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.DELETE)
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.UPDATE_AFTER)
        .build();
  }

  @Override
  public DynamicTableSink copy() {
    return new HoodieTableSink(this.conf, this.schema);
  }

  @Override
  public String asSummaryString() {
    return "HoodieTableSink";
  }

  @Override
  public void applyStaticPartition(Map<String, String> partition) {
    // #applyOverwrite should have been invoked.
    if (this.overwrite) {
      final String operationType;
      if (partition.size() > 0) {
        operationType = WriteOperationType.INSERT_OVERWRITE.value();
      } else {
        operationType = WriteOperationType.INSERT_OVERWRITE_TABLE.value();
      }
      this.conf.setString(FlinkOptions.OPERATION, operationType);
    }
  }

  @Override
  public void applyOverwrite(boolean b) {
    this.overwrite = b;
  }
}
