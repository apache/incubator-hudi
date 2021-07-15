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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.bulkinsert.BulkInsertInternalPartitionerFactory;
import org.apache.hudi.execution.bulkinsert.BulkInsertMapFunction;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A spark implementation of {@link AbstractBulkInsertHelper}.
 *
 * @param <T>
 */
@SuppressWarnings("checkstyle:LineLength")
public class SparkBulkInsertHelper<T extends HoodieRecordPayload, R> extends AbstractBulkInsertHelper<T, JavaRDD<HoodieRecord<T>>,
    JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, R> {

  private SparkBulkInsertHelper() {
  }

  private static class BulkInsertHelperHolder {
    private static final SparkBulkInsertHelper SPARK_BULK_INSERT_HELPER = new SparkBulkInsertHelper();
  }

  public static SparkBulkInsertHelper newInstance() {
    return BulkInsertHelperHolder.SPARK_BULK_INSERT_HELPER;
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> bulkInsert(final JavaRDD<HoodieRecord<T>> inputRecords,
                                                              final String instantTime,
                                                              final HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table,
                                                              final HoodieWriteConfig config,
                                                              final BaseCommitActionExecutor<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, R> executor,
                                                              final boolean performDedupe,
                                                              final Option<BulkInsertPartitioner<T>> userDefinedBulkInsertPartitioner) {
    HoodieWriteMetadata result = new HoodieWriteMetadata();

    //transition bulk_insert state to inflight
    table.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(HoodieInstant.State.REQUESTED,
            table.getMetaClient().getCommitActionType(), instantTime), Option.empty(),
        config.shouldAllowMultiWriteOnSameInstant());
    // write new files
    JavaRDD<WriteStatus> writeStatuses = bulkInsert(inputRecords, instantTime, table, config, performDedupe, userDefinedBulkInsertPartitioner, false, config.getBulkInsertShuffleParallelism(), false);
    //update index
    ((BaseSparkCommitActionExecutor) executor).updateIndexAndCommitIfNeeded(writeStatuses, result);
    return result;
  }

  @Override
  public JavaRDD<WriteStatus> bulkInsert(JavaRDD<HoodieRecord<T>> inputRecords,
                                         String instantTime,
                                         HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table,
                                         HoodieWriteConfig config,
                                         boolean performDedupe,
                                         Option<BulkInsertPartitioner<T>> userDefinedBulkInsertPartitioner,
                                         boolean useWriterSchema,
                                         int parallelism,
                                         boolean preserveMetadata) {

    // De-dupe/merge if needed
    JavaRDD<HoodieRecord<T>> dedupedRecords = inputRecords;

    if (performDedupe) {
      dedupedRecords = (JavaRDD<HoodieRecord<T>>) SparkWriteHelper.newInstance().combineOnCondition(config.shouldCombineBeforeInsert(), inputRecords,
         parallelism, table);
    }

    final JavaRDD<HoodieRecord<T>> repartitionedRecords;
    BulkInsertPartitioner partitioner = userDefinedBulkInsertPartitioner.isPresent()
        ? userDefinedBulkInsertPartitioner.get()
        : BulkInsertInternalPartitionerFactory.get(config.getBulkInsertSortMode());
    repartitionedRecords = (JavaRDD<HoodieRecord<T>>) partitioner.repartitionRecords(dedupedRecords, parallelism);

    // generate new file ID prefixes for each output partition
    final List<String> fileIDPrefixes =
        IntStream.range(0, parallelism).mapToObj(i -> FSUtils.createNewFileIdPfx()).collect(Collectors.toList());

    JavaRDD<WriteStatus> writeStatusRDD = repartitionedRecords
        .mapPartitionsWithIndex(new BulkInsertMapFunction<T>(instantTime,
            partitioner.arePartitionRecordsSorted(), config, table, fileIDPrefixes, useWriterSchema, preserveMetadata), true)
        .flatMap(List::iterator);

    return writeStatusRDD;
  }
}
