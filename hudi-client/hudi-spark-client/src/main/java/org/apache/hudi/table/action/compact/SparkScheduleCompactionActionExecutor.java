/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.action.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@SuppressWarnings("checkstyle:LineLength")
public class SparkScheduleCompactionActionExecutor<T extends HoodieRecordPayload> extends
    BaseScheduleCompactionActionExecutor<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  private static final Logger LOG = LogManager.getLogger(SparkScheduleCompactionActionExecutor.class);

  public SparkScheduleCompactionActionExecutor(HoodieEngineContext context,
                                               HoodieWriteConfig config,
                                               HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table,
                                               String instantTime,
                                               Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime, extraMetadata);
  }

  @Override
  protected HoodieCompactionPlan scheduleCompaction() {
    LOG.info("Checking if compaction needs to be run on " + config.getBasePath());
    // judge if we need to compact according to num delta commits and time elapsed
    boolean compactable = needCompact(config.getInlineCompactTriggerStrategy());
    if (compactable) {
      LOG.info("Generating compaction plan for merge on read table " + config.getBasePath());
      HoodieSparkMergeOnReadTableCompactor compactor = new HoodieSparkMergeOnReadTableCompactor();
      try {
        SyncableFileSystemView fileSystemView = (SyncableFileSystemView) table.getSliceView();
        Set<HoodieFileGroupId> fgInPendingCompactionAndClustering = fileSystemView.getPendingCompactionOperations()
            .map(instantTimeOpPair -> instantTimeOpPair.getValue().getFileGroupId())
            .collect(Collectors.toSet());
        // exclude files in pending clustering from compaction.
        fgInPendingCompactionAndClustering.addAll(fileSystemView.getFileGroupsInPendingClustering().map(Pair::getLeft).collect(Collectors.toSet()));
        return compactor.generateCompactionPlan(context, table, config, instantTime, fgInPendingCompactionAndClustering);
      } catch (IOException e) {
        throw new HoodieCompactionException("Could not schedule compaction " + config.getBasePath(), e);
      }
    }

    return new HoodieCompactionPlan();
  }

  public Tuple2<Integer, String> getLastDeltaCommitInfo(CompactionTriggerStrategy compactionTriggerStrategy) {
    Option<HoodieInstant> lastCompaction = table.getActiveTimeline().getCommitTimeline()
        .filterCompletedInstants().lastInstant();
    HoodieTimeline deltaCommits = table.getActiveTimeline().getDeltaCommitTimeline();

    String lastCompactionTs;
    int deltaCommitsSinceLastCompaction = 0;
    if (lastCompaction.isPresent()) {
      lastCompactionTs = lastCompaction.get().getTimestamp();
    } else {
      lastCompactionTs = deltaCommits.firstInstant().get().getTimestamp();
    }
    if (compactionTriggerStrategy != CompactionTriggerStrategy.TIME_ELAPSED) {
      if (lastCompaction.isPresent()) {
        deltaCommitsSinceLastCompaction = deltaCommits.findInstantsAfter(lastCompactionTs, Integer.MAX_VALUE).countInstants();
      } else {
        deltaCommitsSinceLastCompaction = deltaCommits.findInstantsAfterOrEquals(lastCompactionTs, Integer.MAX_VALUE).countInstants();
      }
    }
    return new Tuple2(deltaCommitsSinceLastCompaction, lastCompactionTs);
  }

  public boolean needCompact(CompactionTriggerStrategy compactionTriggerStrategy) {
    boolean compactable;
    // return deltaCommitsSinceLastCompaction and lastCompactionTs
    Tuple2<Integer, String> threshold = getLastDeltaCommitInfo(compactionTriggerStrategy);
    int inlineCompactDeltaCommitMax = config.getInlineCompactDeltaCommitMax();
    int inlineCompactDeltaElapsedTimeMax = config.getInlineCompactDeltaElapsedTimeMax();
    long elapsedTime;
    switch (compactionTriggerStrategy) {
      case NUM:
        compactable = inlineCompactDeltaCommitMax <= threshold._1;
        if (compactable) {
          LOG.info(String.format("The delta commits >= %s, trigger compaction scheduler.", inlineCompactDeltaCommitMax));
        } else {
          LOG.info(String.format("Not scheduling compaction because %s delta commits needed since last compaction %s."
              + "But only %s delta commits found.", inlineCompactDeltaCommitMax, threshold._2, threshold._1));
        }
        return compactable;
      case TIME_ELAPSED:
        elapsedTime = parsedToSeconds(instantTime) - parsedToSeconds(threshold._2);
        compactable = inlineCompactDeltaElapsedTimeMax <= elapsedTime;
        if (compactable) {
          LOG.info(String.format("The elapsed time >=%ss, trigger compaction scheduler.", inlineCompactDeltaElapsedTimeMax));
        } else {
          LOG.info(String.format("Not scheduling compaction because %s elapsed time needed since last compaction %s."
              + "But only %ss elapsed time found", inlineCompactDeltaElapsedTimeMax, threshold._2, elapsedTime));
        }
        return compactable;
      case NUM_OR_TIME:
        elapsedTime = parsedToSeconds(instantTime) - parsedToSeconds(threshold._2);
        compactable = inlineCompactDeltaCommitMax <= threshold._1 || inlineCompactDeltaElapsedTimeMax <= elapsedTime;
        if (compactable) {
          LOG.info(String.format("The delta commits >= %s or elapsed_time >=%ss, trigger compaction scheduler.", inlineCompactDeltaCommitMax,
              inlineCompactDeltaElapsedTimeMax));
        } else {
          LOG.info(String.format("Not scheduling compaction because %s delta commits or %ss elapsed time needed since last compaction %s."
                  + "But only %s delta commits and %ss elapsed time found", inlineCompactDeltaCommitMax, inlineCompactDeltaElapsedTimeMax, threshold._2,
              threshold._1, elapsedTime));
        }
        return compactable;
      case NUM_AND_TIME:
        elapsedTime = parsedToSeconds(instantTime) - parsedToSeconds(threshold._2);
        compactable = inlineCompactDeltaCommitMax <= threshold._1 && inlineCompactDeltaElapsedTimeMax <= elapsedTime;
        if (compactable) {
          LOG.info(String.format("The delta commits >= %s and elapsed_time >=%ss, trigger compaction scheduler.", inlineCompactDeltaCommitMax,
              inlineCompactDeltaElapsedTimeMax));
        } else {
          LOG.info(String.format("Not scheduling compaction because %s delta commits and %ss elapsed time needed since last compaction %s."
                  + "But only %s delta commits and %ss elapsed time found", inlineCompactDeltaCommitMax, inlineCompactDeltaElapsedTimeMax, threshold._2,
              threshold._1, elapsedTime));
        }
        return compactable;
      default:
        throw new HoodieCompactionException("Unsupported compact type: " + config.getInlineCompactTriggerStrategy());
    }
  }

  public Long parsedToSeconds(String time) {
    long timestamp;
    try {
      timestamp = HoodieActiveTimeline.COMMIT_FORMATTER.parse(time).getTime() / 1000;
    } catch (ParseException e) {
      throw new HoodieCompactionException(e.getMessage(), e);
    }
    return timestamp;
  }
}
