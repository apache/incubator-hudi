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

package org.apache.hudi.sink.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.CleanFunction;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Function to check and commit the compaction action.
 *
 * <p> Each time after receiving a compaction commit event {@link CompactionCommitEvent},
 * it loads and checks the compaction plan {@link HoodieCompactionPlan},
 * if all the compaction operations {@link org.apache.hudi.common.model.CompactionOperation}
 * of the plan are finished, tries to commit the compaction action.
 *
 * <p>It also inherits the {@link CleanFunction} cleaning ability. This is needed because
 * the SQL API does not allow multiple sinks in one table sink provider.
 */
public class CompactionCommitSink extends CleanFunction<CompactionCommitEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(CompactionCommitSink.class);

  /**
   * Config options.
   */
  private final Configuration conf;

  /**
   * Buffer to collect the event from each compact task {@code CompactFunction}.
   * The key is the instant time.
   */
  private transient Map<String, List<CompactionCommitEvent>> commitBuffer;

  public CompactionCommitSink(Configuration conf) {
    super(conf);
    this.conf = conf;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    if (writeClient == null) {
      this.writeClient = StreamerUtil.createWriteClient(conf, getRuntimeContext());
      this.writeClient.registerMetricsGroup(conf.getString(FlinkOptions.TABLE_NAME), getClass().getSimpleName());
    }
    this.commitBuffer = new HashMap<>();
  }

  @Override
  public void invoke(CompactionCommitEvent event, Context context) throws Exception {
    final String instant = event.getInstant();
    commitBuffer.computeIfAbsent(instant, k -> new ArrayList<>())
        .add(event);
    commitIfNecessary(instant, commitBuffer.get(instant));
  }

  /**
   * Condition to commit: the commit buffer has equal size with the compaction plan operations
   * and all the compact commit event {@link CompactionCommitEvent} has the same compaction instant time.
   *
   * @param instant Compaction commit instant time
   * @param events  Commit events ever received for the instant
   */
  private void commitIfNecessary(String instant, List<CompactionCommitEvent> events) throws IOException {
    HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan(
        this.writeClient.getHoodieTable().getMetaClient(), instant);
    boolean isReady = compactionPlan.getOperations().size() == events.size();
    if (!isReady) {
      return;
    }
    List<WriteStatus> statuses = events.stream()
        .map(CompactionCommitEvent::getWriteStatuses)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());

    if (this.writeClient.getConfig().shouldAutoCommit()) {
      // Prepare the commit metadata.
      List<HoodieWriteStat> updateStatusMap = statuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());
      HoodieCommitMetadata metadata = new HoodieCommitMetadata(true);
      for (HoodieWriteStat stat : updateStatusMap) {
        metadata.addWriteStat(stat.getPartitionPath(), stat);
      }
      metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, writeClient.getConfig().getSchema());
      this.writeClient.completeCompaction(
          metadata, statuses, this.writeClient.getHoodieTable(), instant);
    }
    // commit the compaction
    this.writeClient.commitCompaction(instant, statuses, Option.empty());

    // Whether to cleanup the old log file when compaction
    if (!conf.getBoolean(FlinkOptions.CLEAN_ASYNC_ENABLED)) {
      this.writeClient.clean();
    }

    // reset the status
    reset(instant);
  }

  private void reset(String instant) {
    this.commitBuffer.remove(instant);
  }
}
